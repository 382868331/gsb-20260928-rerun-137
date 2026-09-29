package shop

import (
	"fmt"

	"replayable-app"
)

const (
	EventItemAdded   = "shop.item_added"
	EventOrderPlaced = "shop.order_placed"
	EventOrderPaid   = "shop.order_paid"
)

type ItemAdded struct {
	SKU      string `json:"sku"`
	Name     string `json:"name"`
	Quantity int    `json:"quantity"`
}

type OrderPlaced struct {
	OrderID string         `json:"order_id"`
	Items   map[string]int `json:"items"`
}

type OrderPaid struct {
	OrderID string `json:"order_id"`
}

type Order struct {
	ID     string         `json:"id"`
	Items  map[string]int `json:"items"`
	Status string         `json:"status"`
}

type State struct {
	Stock   map[string]int    `json:"stock"`
	Names   map[string]string `json:"names"`
	Orders  map[string]Order  `json:"orders"`
	Revenue int               `json:"revenue"`
}

func NewState() State {
	return State{
		Stock:  make(map[string]int),
		Names:  make(map[string]string),
		Orders: make(map[string]Order),
	}
}

func CloneState(state State) State {
	cloned := State{
		Stock:   make(map[string]int, len(state.Stock)),
		Names:   make(map[string]string, len(state.Names)),
		Orders:  make(map[string]Order, len(state.Orders)),
		Revenue: state.Revenue,
	}
	for sku, quantity := range state.Stock {
		cloned.Stock[sku] = quantity
	}
	for sku, name := range state.Names {
		cloned.Names[sku] = name
	}
	for orderID, order := range state.Orders {
		cloned.Orders[orderID] = CloneOrder(order)
	}
	return cloned
}

func CloneOrder(order Order) Order {
	items := make(map[string]int, len(order.Items))
	for sku, quantity := range order.Items {
		items[sku] = quantity
	}
	order.Items = items
	return order
}

func Apply(state *State, event replay.RecordedEvent) error {
	switch event.Type {
	case EventItemAdded:
		payload, err := replay.DecodeEvent[ItemAdded](event)
		if err != nil {
			return err
		}
		if payload.Quantity <= 0 {
			return fmt.Errorf("item %q quantity must be positive", payload.SKU)
		}
		state.Stock[payload.SKU] += payload.Quantity
		state.Names[payload.SKU] = payload.Name
	case EventOrderPlaced:
		payload, err := replay.DecodeEvent[OrderPlaced](event)
		if err != nil {
			return err
		}
		total := 0
		for sku, quantity := range payload.Items {
			if quantity <= 0 {
				return fmt.Errorf("order %q item %q quantity must be positive", payload.OrderID, sku)
			}
			if state.Stock[sku] < quantity {
				return fmt.Errorf("insufficient stock for %q", sku)
			}
			total += quantity
		}
		if _, exists := state.Orders[payload.OrderID]; exists {
			return fmt.Errorf("order %q already exists", payload.OrderID)
		}
		for sku, quantity := range payload.Items {
			state.Stock[sku] -= quantity
		}
		state.Orders[payload.OrderID] = Order{ID: payload.OrderID, Items: payload.Items, Status: "placed"}
	case EventOrderPaid:
		payload, err := replay.DecodeEvent[OrderPaid](event)
		if err != nil {
			return err
		}
		order, exists := state.Orders[payload.OrderID]
		if !exists {
			return fmt.Errorf("order %q not found", payload.OrderID)
		}
		if order.Status != "placed" {
			return fmt.Errorf("order %q is %s", payload.OrderID, order.Status)
		}
		for _, quantity := range order.Items {
			state.Revenue += quantity * 10
		}
		order.Status = "paid"
		state.Orders[payload.OrderID] = order
	default:
		return fmt.Errorf("unknown event type %q", event.Type)
	}
	return nil
}
