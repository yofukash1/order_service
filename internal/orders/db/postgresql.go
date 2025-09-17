package orders

import (
	"context"
	"fmt"
	"log"
	"wb_tech/L0/internal/orders"
	"wb_tech/L0/pkg/database/postgres"

	"github.com/jackc/pgconn"
)

type repository struct {
	client postgres.PostgresClient
}

func NewRepository(client postgres.PostgresClient) orders.Repository {
	return &repository{client: client}
}

// Методы для работы в транзакции
func (r *repository) createPaymentTx(ctx context.Context, tx postgres.PostgresClient, payment *orders.Payment) (string, error) {
	q := `
		INSERT INTO payments
			(transaction_id,
			request_id,
			currency,	
			provider,
			amount,
			payment_dt,
			bank,
			delivery_cost,
			goods_total,
			custom_fee)
		VALUES 
			($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT DO NOTHING`

	_, err := tx.Exec(ctx, q, payment.Transaction, payment.RequestID, payment.Currency, payment.Provider,
		payment.Amount, payment.PaymentDT, payment.Bank, payment.DeliveryCost, payment.GoodsTotal, payment.CustomFee)

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			newErr := fmt.Errorf("SQL Error: %s Where: %s Detail: %s Code: %s SQLState: %s",
				pgErr.Message, pgErr.Where, pgErr.Detail, pgErr.Code, pgErr.SQLState())
			log.Println(newErr)
			return "", newErr
		}
		return "", err
	}
	return payment.Transaction, nil
}

func (r *repository) createDeliveryTx(ctx context.Context, tx postgres.PostgresClient, delivery *orders.Delivery) (int, error) {
	q := `
		INSERT INTO deliveries (
			name, phone, zip, city, address, region, email
		) VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING
		RETURNING id`

	var deliveryID int
	err := tx.QueryRow(ctx, q,
		delivery.Name,
		delivery.Phone,
		delivery.Zip,
		delivery.City,
		delivery.Address,
		delivery.Region,
		delivery.Email,
	).Scan(&deliveryID)

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			newErr := fmt.Errorf("SQL Error: %s Where: %s Detail: %s Code: %s SQLState: %s",
				pgErr.Message, pgErr.Where, pgErr.Detail, pgErr.Code, pgErr.SQLState())
			log.Println(newErr)
			return 0, newErr
		}
		return 0, err
	}
	return deliveryID, nil
}

func (r *repository) createItemTx(ctx context.Context, tx postgres.PostgresClient, orderUID string, item *orders.Item) error {
	q := `
		INSERT INTO items (
			order_uid, chrt_id, track_number, price, rid, name, 
			sale, size, total_price, nm_id, brand, status
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT DO NOTHING`

	_, err := tx.Exec(ctx, q,
		orderUID,
		item.ChrtID,
		item.TrackNumber,
		item.Price,
		item.RID,
		item.Name,
		item.Sale,
		item.Size,
		item.TotalPrice,
		item.NmID,
		item.Brand,
		item.Status,
	)

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			newErr := fmt.Errorf("SQL Error: %s Where: %s Detail: %s Code: %s SQLState: %s",
				pgErr.Message, pgErr.Where, pgErr.Detail, pgErr.Code, pgErr.SQLState())
			log.Println(newErr)
			return newErr
		}
		return err
	}
	return nil
}

func (r *repository) CreateOrder(ctx context.Context, order *orders.Order) error {
	log.Println("SQL: BEGIN TRANSACTION")
	// Start transaction
	tx, err := r.client.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	paymentID, err := r.createPaymentTx(ctx, tx, &order.Payment)
	if err != nil {
		return fmt.Errorf("failed to create payment: %w", err)
	}

	deliveryID, err := r.createDeliveryTx(ctx, tx, &order.Delivery)
	if err != nil {
		return fmt.Errorf("failed to create delivery: %w", err)
	}

	orderQ := `
		INSERT INTO orders (
			order_uid, track_number, entry, delivery_id, payment_id,
			locale, internal_signature, customer_id, delivery_service,
			shardkey, sm_id, date_created, oof_shard
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (order_uid) DO UPDATE SET
			track_number = EXCLUDED.track_number,
			entry = EXCLUDED.entry,
			delivery_id = EXCLUDED.delivery_id,
			payment_id = EXCLUDED.payment_id,
			locale = EXCLUDED.locale,
			internal_signature = EXCLUDED.internal_signature,
			customer_id = EXCLUDED.customer_id,
			delivery_service = EXCLUDED.delivery_service,
			shardkey = EXCLUDED.shardkey,
			sm_id = EXCLUDED.sm_id,
			date_created = EXCLUDED.date_created,
			oof_shard = EXCLUDED.oof_shard,
			updated_at = CURRENT_TIMESTAMP`

	_, err = tx.Exec(ctx, orderQ,
		order.OrderUID,
		order.TrackNumber,
		order.Entry,
		deliveryID,
		paymentID,
		order.Locale,
		order.InternalSignature,
		order.CustomerID,
		order.DeliveryService,
		order.ShardKey,
		order.SmID,
		order.DateCreated,
		order.OofShard,
	)

	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok {
			return fmt.Errorf("SQL Error: %s Where: %s Detail: %s Code: %s SQLState: %s",
				pgErr.Message, pgErr.Where, pgErr.Detail, pgErr.Code, pgErr.SQLState())
		}
		return fmt.Errorf("failed to create order: %w", err)
	}

	for _, item := range order.Items {
		log.Println("Trying to add items")
		err := r.createItemTx(ctx, tx, order.OrderUID, &item)
		if err != nil {
			return fmt.Errorf("failed to create item: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	log.Println("SQL: COMMIT TRANSACTION")
	return nil
}

// Оригинальные методы для отдельных операций (no transaction!)
func (r *repository) CreatePayment(ctx context.Context, payment *orders.Payment) (transaction string, err error) {
	return r.createPaymentTx(ctx, r.client, payment)
}

func (r *repository) CreateItem(ctx context.Context, orderUID string, item *orders.Item) error {
	return r.createItemTx(ctx, r.client, orderUID, item)
}

func (r *repository) CreateDelivery(ctx context.Context, delivery *orders.Delivery) (deliveryID int, err error) {
	return r.createDeliveryTx(ctx, r.client, delivery)
}

// GetOrderByUID retrieves a complete order with all related data
func (r *repository) GetOrderByUID(ctx context.Context, orderUID string) (*orders.Order, error) {
	q := `
		SELECT 
			o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature,
			o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
			d.name, d.phone, d.zip, d.city, d.address, d.region, d.email,
			p.transaction_id, p.request_id, p.currency, p.provider, p.amount, p.payment_dt,
			p.bank, p.delivery_cost, p.goods_total, p.custom_fee
		FROM orders o
		JOIN deliveries d ON o.delivery_id = d.id
		JOIN payments p ON o.payment_id = p.transaction_id
		WHERE o.order_uid = $1`

	var order orders.Order
	var delivery orders.Delivery
	var payment orders.Payment

	err := r.client.QueryRow(ctx, q, orderUID).Scan(
		&order.OrderUID, &order.TrackNumber, &order.Entry, &order.Locale, &order.InternalSignature,
		&order.CustomerID, &order.DeliveryService, &order.ShardKey, &order.SmID, &order.DateCreated, &order.OofShard,
		&delivery.Name, &delivery.Phone, &delivery.Zip, &delivery.City, &delivery.Address, &delivery.Region, &delivery.Email,
		&payment.Transaction, &payment.RequestID, &payment.Currency, &payment.Provider, &payment.Amount, &payment.PaymentDT,
		&payment.Bank, &payment.DeliveryCost, &payment.GoodsTotal, &payment.CustomFee,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get order: %w", err)
	}

	order.Delivery = delivery
	order.Payment = payment

	// Get items
	itemsQ := `
		SELECT chrt_id, track_number, price, rid, name, sale, size, 
		       total_price, nm_id, brand, status
		FROM items WHERE order_uid = $1`

	rows, err := r.client.Query(ctx, itemsQ, orderUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item orders.Item
		err := rows.Scan(
			&item.ChrtID, &item.TrackNumber, &item.Price, &item.RID, &item.Name,
			&item.Sale, &item.Size, &item.TotalPrice, &item.NmID, &item.Brand, &item.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan item: %w", err)
		}
		order.Items = append(order.Items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating items: %w", err)
	}

	return &order, nil
}
