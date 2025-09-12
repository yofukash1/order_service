package orders

import (
	"context"
)

type Repository interface {
	CreateOrder(ctx context.Context, order *Order) error
}
