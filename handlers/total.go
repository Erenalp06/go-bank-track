package handlers

import (
	"github.com/Erenalp06/go-bank-track/services"
	"github.com/gofiber/fiber/v2"
)

func GetTotal(esService *services.ElasticsearchService) fiber.Handler {
	return func(c *fiber.Ctx) error {
		bankID := c.Params("bank_id")

		data, err := esService.QueryData()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		totalData := services.ProcessTotalData(data, bankID)
		return c.JSON(totalData)
	}
}
