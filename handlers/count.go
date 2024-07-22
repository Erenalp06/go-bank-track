package handlers

import (
	"github.com/Erenalp06/go-bank-track/services"
	"github.com/gofiber/fiber/v2"
)

func GetCount(esService *services.ElasticsearchService) fiber.Handler {
	return func(c *fiber.Ctx) error {
		bankID := c.Params("bankID")

		data, err := esService.QueryData()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		countData := services.ProcessCountData(data, bankID)

		return c.JSON(countData)
	}
}

func GetTransferCount(esService *services.ElasticsearchService) fiber.Handler {
	return func(c *fiber.Ctx) error {
		bankID := c.Params("bankID")

		data, err := esService.QueryData()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		countData := services.ProcessTransferData(data, bankID)

		return c.JSON(countData)
	}
}

func GetTransactionsBetweenBanks(esService *services.ElasticsearchService) fiber.Handler {
	return func(c *fiber.Ctx) error {
		bankID1 := c.Params("bankID1")
		bankID2 := c.Params("bankID2")

		if bankID1 == "" || bankID2 == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Both bankID1 and bankID2 are required in the URL path"})
		}

		data, err := esService.QueryData() // Elasticsearch'den veriyi sorgular
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to retrieve data"})
		}

		count := services.CountTransactionsBetweenBanks(data, bankID1, bankID2)
		return c.JSON(fiber.Map{"transactionCount": count, "fromBankID": bankID1, "toBankID": bankID2})
	}
}

func GetTransactionsByDate(esService *services.ElasticsearchService) fiber.Handler {
	return func(c *fiber.Ctx) error {
		bankID := c.Params("bankID")
		startDate := c.Query("start")
		endDate := c.Query("end")

		if bankID == "" || startDate == "" || endDate == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "BankID, start date, and end date are required"})
		}

		data, err := esService.QueryData()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to retrieve data"})
		}

		countData := services.CountTransactionsByDate(data, bankID, startDate, endDate)
		return c.JSON(fiber.Map{
			"transactionCount": countData,
			"bankID":           bankID,
			"startDate":        startDate,
			"endDate":          endDate,
		})
	}
}
