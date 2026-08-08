package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/ewhauser/rivet-go/rivet"
)

type inventoryInput struct {
	InitialStock int    `json:"initialStock"`
	ItemName     string `json:"itemName"`
}

type inventoryState struct {
	ItemName     string         `json:"itemName"`
	Stock        int            `json:"stock"`
	Reservations map[string]int `json:"reservations"`
}

type stockResult struct {
	ItemName string `json:"itemName"`
	Stock    int    `json:"stock"`
}

type reservationArgs struct {
	CheckoutID string `json:"checkoutId"`
	Quantity   int    `json:"quantity"`
}

type reservationResult struct {
	Success        bool   `json:"success"`
	Message        string `json:"message,omitempty"`
	AvailableStock int    `json:"availableStock,omitempty"`
	RemainingStock int    `json:"remainingStock,omitempty"`
}

type checkoutInput struct {
	CustomerName string `json:"customerName"`
}

type checkoutItem struct {
	ItemID   string `json:"itemId"`
	ItemName string `json:"itemName"`
	Quantity int    `json:"quantity"`
}

type checkoutState struct {
	CustomerName string         `json:"customerName"`
	Items        []checkoutItem `json:"items"`
	Completed    bool           `json:"completed"`
}

type addItemArgs struct {
	ItemID   string `json:"itemId"`
	Quantity int    `json:"quantity"`
}

type checkoutResult struct {
	Success        bool   `json:"success"`
	Message        string `json:"message"`
	RemainingStock int    `json:"remainingStock,omitempty"`
}

type checkoutSummary struct {
	CustomerName string         `json:"customerName"`
	Items        []checkoutItem `json:"items"`
	Completed    bool           `json:"completed"`
	TotalItems   int            `json:"totalItems"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "cross-actor-actions:", err)
		os.Exit(1)
	}
}

func run() error {
	var endpoint, runnerName, token string
	flag.StringVar(&endpoint, "endpoint", "http://127.0.0.1:6420", "Rivet Engine endpoint")
	flag.StringVar(&runnerName, "runner-name", "cross-actor-actions-example", "engine-visible runner name")
	flag.StringVar(&token, "token", "dev", "Rivet Engine bearer token for actor-to-actor calls")
	flag.Parse()

	registry := rivet.NewRegistry()
	if err := registerCrossActorActions(registry); err != nil {
		return err
	}
	return rivet.Serve(registry, rivet.Config{
		Endpoint:   endpoint,
		RunnerName: runnerName,
		Token:      token,
		TotalSlots: 16,
		LogLevel:   "info",
		Logger:     slog.New(slog.NewJSONHandler(os.Stderr, nil)),
	})
}

func registerCrossActorActions(registry *rivet.Registry) error {
	if err := rivet.Register(registry, "inventory", rivet.Actor[inventoryState]{
		OnStart: func(ctx *rivet.Context[inventoryState]) error {
			if ctx.State().ItemName != "" {
				if ctx.State().Reservations == nil {
					ctx.State().Reservations = make(map[string]int)
				}
				return nil
			}
			var input inventoryInput
			if err := decodeJSONInput(ctx.Input(), &input); err != nil {
				return fmt.Errorf("decode inventory input: %w", err)
			}
			if strings.TrimSpace(input.ItemName) == "" || input.InitialStock < 0 {
				return errors.New("itemName is required and initialStock must not be negative")
			}
			ctx.State().ItemName = input.ItemName
			ctx.State().Stock = input.InitialStock
			ctx.State().Reservations = make(map[string]int)
			return nil
		},
		Actions: rivet.Actions[inventoryState]{
			"getStock": rivet.Action(func(ctx *rivet.Context[inventoryState], _ struct{}) (stockResult, error) {
				return stockResult{ItemName: ctx.State().ItemName, Stock: ctx.State().Stock}, nil
			}),
			"reserveItems": rivet.Action(func(ctx *rivet.Context[inventoryState], args reservationArgs) (reservationResult, error) {
				if args.Quantity <= 0 {
					return reservationResult{}, rivet.ActionError{Code: "invalid_quantity", Message: "quantity must be positive"}
				}
				if ctx.State().Stock < args.Quantity {
					return reservationResult{
						Success:        false,
						Message:        fmt.Sprintf("Insufficient stock. Available: %d, Requested: %d", ctx.State().Stock, args.Quantity),
						AvailableStock: ctx.State().Stock,
					}, nil
				}
				ctx.State().Stock -= args.Quantity
				ctx.State().Reservations[args.CheckoutID] += args.Quantity
				return reservationResult{
					Success:        true,
					Message:        fmt.Sprintf("Reserved %d items for checkout %s", args.Quantity, args.CheckoutID),
					RemainingStock: ctx.State().Stock,
				}, nil
			}),
			"releaseItems": rivet.Action(func(ctx *rivet.Context[inventoryState], args reservationArgs) (reservationResult, error) {
				reserved := ctx.State().Reservations[args.CheckoutID]
				if reserved > 0 {
					release := min(args.Quantity, reserved)
					ctx.State().Stock += release
					if release == reserved {
						delete(ctx.State().Reservations, args.CheckoutID)
					} else {
						ctx.State().Reservations[args.CheckoutID] = reserved - release
					}
				}
				return reservationResult{Success: true, RemainingStock: ctx.State().Stock}, nil
			}),
		},
	}); err != nil {
		return err
	}

	return rivet.Register(registry, "checkout", rivet.Actor[checkoutState]{
		OnStart: func(ctx *rivet.Context[checkoutState]) error {
			if ctx.State().CustomerName != "" {
				return nil
			}
			var input checkoutInput
			if err := decodeJSONInput(ctx.Input(), &input); err != nil {
				return fmt.Errorf("decode checkout input: %w", err)
			}
			if strings.TrimSpace(input.CustomerName) == "" {
				return errors.New("customerName is required")
			}
			ctx.State().CustomerName = input.CustomerName
			ctx.State().Items = []checkoutItem{}
			return nil
		},
		Actions: rivet.Actions[checkoutState]{
			"addItem": rivet.ActionWithContext(func(actionContext context.Context, ctx *rivet.Context[checkoutState], args addItemArgs) (checkoutResult, error) {
				if args.ItemID == "" || args.Quantity <= 0 {
					return checkoutResult{}, rivet.ActionError{Code: "invalid_item", Message: "itemId is required and quantity must be positive"}
				}
				inventory, err := resolveInventory(actionContext, ctx.Client(), args.ItemID)
				if err != nil {
					return checkoutResult{}, fmt.Errorf("resolve inventory: %w", err)
				}
				item, err := rivet.Call[stockResult](actionContext, inventory, "getStock", struct{}{})
				if err != nil {
					return checkoutResult{}, fmt.Errorf("get inventory stock: %w", err)
				}
				reservation, err := rivet.Call[reservationResult](actionContext, inventory, "reserveItems", reservationArgs{
					CheckoutID: ctx.ActorID(),
					Quantity:   args.Quantity,
				})
				if err != nil {
					return checkoutResult{}, fmt.Errorf("reserve inventory: %w", err)
				}
				if !reservation.Success {
					return checkoutResult{Success: false, Message: reservation.Message}, nil
				}
				ctx.State().Items = append(ctx.State().Items, checkoutItem{
					ItemID: args.ItemID, ItemName: item.ItemName, Quantity: args.Quantity,
				})
				return checkoutResult{
					Success:        true,
					Message:        fmt.Sprintf("Added %d %s to checkout", args.Quantity, item.ItemName),
					RemainingStock: reservation.RemainingStock,
				}, nil
			}),
			"getSummary": rivet.Action(func(ctx *rivet.Context[checkoutState], _ struct{}) (checkoutSummary, error) {
				total := 0
				for _, item := range ctx.State().Items {
					total += item.Quantity
				}
				return checkoutSummary{
					CustomerName: ctx.State().CustomerName,
					Items:        append([]checkoutItem(nil), ctx.State().Items...),
					Completed:    ctx.State().Completed,
					TotalItems:   total,
				}, nil
			}),
			"completeCheckout": rivet.Action(func(ctx *rivet.Context[checkoutState], _ struct{}) (checkoutResult, error) {
				ctx.State().Completed = true
				return checkoutResult{Success: true, Message: "Checkout completed successfully"}, nil
			}),
			"cancelCheckout": rivet.ActionWithContext(func(actionContext context.Context, ctx *rivet.Context[checkoutState], _ struct{}) (checkoutResult, error) {
				for _, item := range ctx.State().Items {
					inventory, err := resolveInventory(actionContext, ctx.Client(), item.ItemID)
					if err != nil {
						return checkoutResult{}, fmt.Errorf("resolve inventory %q: %w", item.ItemID, err)
					}
					if _, err := rivet.Call[reservationResult](actionContext, inventory, "releaseItems", reservationArgs{
						CheckoutID: ctx.ActorID(), Quantity: item.Quantity,
					}); err != nil {
						return checkoutResult{}, fmt.Errorf("release inventory %q: %w", item.ItemID, err)
					}
				}
				ctx.State().Items = []checkoutItem{}
				return checkoutResult{Success: true, Message: "Checkout cancelled, items returned to inventory"}, nil
			}),
		},
	})
}

func decodeJSONInput(input []byte, output any) error {
	if len(input) == 0 {
		return errors.New("creation input is required")
	}
	return json.Unmarshal(input, output)
}

func resolveInventory(ctx context.Context, client *rivet.Client, itemID string) (*rivet.ActorHandle, error) {
	input, err := json.Marshal(inventoryInput{ItemName: itemID, InitialStock: 0})
	if err != nil {
		return nil, err
	}
	actor, _, err := client.GetOrCreate(ctx, "inventory", []string{itemID}, rivet.CreateOptions{Input: input})
	return actor, err
}
