package main

import (
	"log"

	"github.com/Erenalp06/go-bank-track/handlers"
	"github.com/Erenalp06/go-bank-track/services"
	"github.com/gofiber/fiber/v2"
)

func main() {
	app := fiber.New()

	esService, err := services.NewElasticSearchService()
	if err != nil {
		log.Fatalf("Error creating the Elasticsearch service: %s", err)
	}

	app.Get("/bank/:bankID/transactions/total", handlers.GetTotal(esService))
	app.Get("/bank/:bankID/transactions/count", handlers.GetCount(esService))
	app.Get("/bank/:bankID/transactions/count/transfer", handlers.GetTransferCount(esService))
	app.Get("/transactions/from/:bankID1/to/:bankID2", handlers.GetTransactionsBetweenBanks(esService))
	app.Get("/bank/:bankID/transactions/date/count", handlers.GetTransactionsByDate(esService))
	app.Get("/bank/:bankID/transactions/exception", handlers.GetException(esService))
	app.Get("transactions/slowest", handlers.GetSlowestTransactions(esService))
	app.Get("transactions/percentiles", handlers.GetPercentilesTransactions(esService))

	log.Fatal(app.Listen(":3000"))
}
