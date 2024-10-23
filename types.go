package main

type Message struct {
	Payload interface{}
}

type AppPayment struct {
	Timestamp       string `json:"timestamp"`
	BuyerAccountId  string `json:"buyer_account_id"`
	SellerAccountId string `json:"seller_account_id"`
	AssetCode       string `json:"asset_code"`
	Amount          string `json:"amount"`
	Type            string `json:"type"`
	Memo            string `json:"memo"`
}

type CreateAccountOp struct {
	Timestamp       string `json:"timestamp"`
	Funder          string `json:"funder"`
	Account         string `json:"account"`
	StartingBalance string `json:"starting_balance"`
	Type            string `json:"type"`
}
