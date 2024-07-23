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

func GetPercentilesTransactions(es *services.ElasticsearchService) fiber.Handler {
	return func(c *fiber.Ctx) error {

		query := `{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        {"terms": {
						"operationName": [
							"POST /api/v1/transactions/fee",
							"POST /api/v1/transactions/deposit",
							"POST /api/v1/transactions/transfer",
							"POST /api/v1/transactions/withdraw",
							"POST /api/v1/transactions/refund",
							"POST /api/v1/transactions/payment"
						]
					}}
      ]
    }
  },
  "aggs": {
    "by_operation": {
      "terms": {
        "field": "operationName",
        "size": 10
      },
      "aggs": {
        "load_time_percentiles": {
          "percentiles": {
            "field": "duration",
            "percents": [50, 75, 90, 95, 99]
          }
        }
      }
    }
  }
}
`
		data, err := es.QueryDataWithCustomQuery(query)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to retrieve data"})
		}

		percentiles := services.ProcessPercentilesTransactions(data)
		return c.JSON(fiber.Map{
			"percentiles(ms)": percentiles,
		})
	}
}

func GetSlowestTransactions(esService *services.ElasticsearchService) fiber.Handler {
	return func(c *fiber.Ctx) error {

		query := `{
			"size": 0,
			"query": {
				"bool": {
					"must": [
						{"terms": {
							"operationName": [
								"POST /api/v1/transactions/fee",
								"POST /api/v1/transactions/deposit",
								"POST /api/v1/transactions/transfer",
								"POST /api/v1/transactions/withdraw",
								"POST /api/v1/transactions/refund",
								"POST /api/v1/transactions/payment"
							]
						}}
					]
				}
			},
			"aggs": {
				"by_endpoint": {
					"terms": {
						"field": "operationName",
						"size": 10
					},
					"aggs": {
						"top_slow_transactions": {
							"top_hits": {
								"sort": [
									{
										"duration": {
											"order": "desc"
										}
									}
								],
								"_source": {
									"includes": ["operationName", "duration", "tags.http.response.body", "tags.http.request.body"]
								},
								"size": 5
							}
						}
					}
				}
			}
		}`

		data, err := esService.QueryDataWithCustomQuery(query)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Failed to retrieve data"})
		}

		slowestTransactions := services.ProcessTransactionsData(data)
		return c.JSON(fiber.Map{
			"slowestTransactions": slowestTransactions,
		})
	}
}
