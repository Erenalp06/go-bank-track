package services

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/tidwall/gjson"
)

type ElasticsearchService struct {
	client *elasticsearch.Client
}

func NewElasticSearchService() (*ElasticsearchService, error) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return &ElasticsearchService{client: client}, nil
}

func (es *ElasticsearchService) QueryData() (string, error) {
	query := `{
		"query": {
			"bool": {
				"must": [
					{
						"bool": {
							"should": [
								{"match": {"operationName": "POST /api/v1/transactions/fee"}},
								{"match": {"operationName": "POST /api/v1/transactions/deposit"}},
								{"match": {"operationName": "POST /api/v1/transactions/transfer"}},
								{"match": {"operationName": "POST /api/v1/transactions/withdraw"}},
								{"match": {"operationName": "POST /api/v1/transactions/refund"}},
								{"match": {"operationName": "POST /api/v1/transactions/payment"}}
							]
						}
					},
					{"match": {"process.serviceName": "java-bank-api"}},
					{
						"nested": {
							"path": "tags",
							"query": {
								"bool": {
									"must": [
										{"match": {"tags.key": "http.response.body"}}
									]
								}
							}
						}
					}
				]
			}
		},
		"_source": ["operationName", "tags"],
		"from": 0,
		"size": 1000
	}`

	res, err := es.client.Search(
		es.client.Search.WithContext(context.Background()),
		es.client.Search.WithIndex("jaeger-span-2024-07-21"),
		es.client.Search.WithBody(strings.NewReader(query)),
		es.client.Search.WithTrackTotalHits(true),
		es.client.Search.WithPretty(),
	)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return "", fmt.Errorf("error parsing the response body: %s", err)
		} else {
			return "", fmt.Errorf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	var b strings.Builder
	if _, err := io.Copy(&b, res.Body); err != nil {
		return "", err
	}

	return b.String(), nil
}

func ProcessCountData(data, bankID string) map[string]map[string]int {
	countData := make(map[string]map[string]int)

	hits := gjson.Get(data, "hits.hits.#._source")
	hits.ForEach(func(key, value gjson.Result) bool {
		responseBody := ""
		value.Get("tags").ForEach(func(_, tag gjson.Result) bool {
			if tag.Get("key").String() == "http.response.body" {
				responseBody = tag.Get("value").String()
				return false
			}
			return true
		})

		if !gjson.Get(responseBody, "exceptionType").Exists() {
			sourceBankID := gjson.Get(responseBody, "sourceAccount.bank.bankId").String()
			sourceBankName := gjson.Get(responseBody, "sourceAccount.bank.name").String()
			destinationBankID := gjson.Get(responseBody, "destinationAccount.bank.bankId").String()
			destinationBankName := gjson.Get(responseBody, "destinationAccount.bank.name").String()
			transactionType := gjson.Get(responseBody, "transactionType").String()

			if transactionType != "" {
				if sourceBankID == bankID {
					if countData[sourceBankName] == nil {
						countData[sourceBankName] = make(map[string]int)
					}
					countData[sourceBankName][transactionType]++
				}

				if destinationBankID == bankID && destinationBankName != "" {
					if countData[destinationBankName] == nil {
						countData[destinationBankName] = make(map[string]int)
					}
					countData[destinationBankName][transactionType]++
				}
			}
		}

		return true
	})

	return countData
}

func CountTransactionsBetweenBanks(data, bankID1, bankID2 string) int {
	totalTransactions := 0

	hits := gjson.Get(data, "hits.hits.#._source")
	hits.ForEach(func(key, value gjson.Result) bool {
		responseBody := ""
		value.Get("tags").ForEach(func(_, tag gjson.Result) bool {
			if tag.Get("key").String() == "http.response.body" {
				responseBody = tag.Get("value").String()
				return false
			}
			return true
		})

		if !gjson.Get(responseBody, "exceptionType").Exists() {
			sourceBankID := gjson.Get(responseBody, "sourceAccount.bank.bankId").String()
			destinationBankID := gjson.Get(responseBody, "destinationAccount.bank.bankId").String()

			if (sourceBankID == bankID1 && destinationBankID == bankID2) || (sourceBankID == bankID2 && destinationBankID == bankID1) {
				totalTransactions++
			}
		}

		return true
	})

	return totalTransactions
}

func ProcessTransferData(data, bankID string) map[string]map[string]int {
	transferData := make(map[string]map[string]int)

	hits := gjson.Get(data, "hits.hits.#._source")
	hits.ForEach(func(key, value gjson.Result) bool {
		responseBody := ""
		value.Get("tags").ForEach(func(_, tag gjson.Result) bool {
			if tag.Get("key").String() == "http.response.body" {
				responseBody = tag.Get("value").String()
				return false
			}
			return true
		})

		if !gjson.Get(responseBody, "exceptionType").Exists() {
			sourceBankID := gjson.Get(responseBody, "sourceAccount.bank.bankId").String()
			sourceBankName := gjson.Get(responseBody, "sourceAccount.bank.name").String()
			destinationBankID := gjson.Get(responseBody, "destinationAccount.bank.bankId").String()
			destinationBankName := gjson.Get(responseBody, "destinationAccount.bank.name").String()
			transactionType := gjson.Get(responseBody, "transactionType").String()

			if transactionType == "TRANSFER" {
				if sourceBankID == bankID {
					detail := destinationBankName + " (to)"
					if transferData[detail] == nil {
						transferData[detail] = make(map[string]int)
					}
					transferData[detail][transactionType]++
				}

				if destinationBankID == bankID {
					detail := sourceBankName + " (from)"
					if transferData[detail] == nil {
						transferData[detail] = make(map[string]int)
					}
					transferData[detail][transactionType]++
				}
			}
		}

		return true
	})

	return transferData
}

func ProcessTotalData(data, bankID string) int {
	total := 0

	hits := gjson.Get(data, "hits.hits.#._source")
	hits.ForEach(func(key, value gjson.Result) bool {
		responseBody := ""
		value.Get("tags").ForEach(func(_, tag gjson.Result) bool {
			if tag.Get("key").String() == "http.response.body" {
				responseBody = tag.Get("value").String()
				return false
			}
			return true
		})

		if gjson.Get(responseBody, "accountDetails.bank.bankId").String() == bankID {
			total++
		}

		return true
	})

	return total
}

func ProcessExceptionData(data, bankID string) map[string]int {
	exceptionData := make(map[string]int)

	hits := gjson.Get(data, "hits.hits.#._source")
	hits.ForEach(func(key, value gjson.Result) bool {
		responseBody := ""
		value.Get("tags").ForEach(func(_, tag gjson.Result) bool {
			if tag.Get("key").String() == "http.response.body" {
				responseBody = tag.Get("value").String()
				return false
			}
			return true
		})

		if gjson.Get(responseBody, "accountDetails.bank.bankId").String() == bankID {
			if gjson.Get(responseBody, "exceptionType").Exists() {
				exceptionType := gjson.Get(responseBody, "exceptionType").String()
				exceptionData[exceptionType]++
			}
		}

		return true
	})

	return exceptionData
}

func CountTransactionsByDate(data, bankID, startDate, endDate string) map[string]map[string]int {
	countData := make(map[string]map[string]int)

	hits := gjson.Get(data, "hits.hits.#._source")
	hits.ForEach(func(key, value gjson.Result) bool {
		responseBody := ""
		value.Get("tags").ForEach(func(_, tag gjson.Result) bool {
			if tag.Get("key").String() == "http.response.body" {
				responseBody = tag.Get("value").String()
				return false
			}
			return true
		})

		transactionDate := gjson.Get(responseBody, "transactionDate").String()
		if IsDateInRange(transactionDate, startDate, endDate) {
			if !gjson.Get(responseBody, "exceptionType").Exists() {
				sourceBankID := gjson.Get(responseBody, "sourceAccount.bank.bankId").String()
				sourceBankName := gjson.Get(responseBody, "sourceAccount.bank.name").String()
				destinationBankID := gjson.Get(responseBody, "destinationAccount.bank.bankId").String()
				destinationBankName := gjson.Get(responseBody, "destinationAccount.bank.name").String()
				transactionType := gjson.Get(responseBody, "transactionType").String()

				var bankName string
				if sourceBankID == bankID {
					bankName = sourceBankName
				} else {
					bankName = destinationBankName
				}

				if sourceBankID == bankID || destinationBankID == bankID {
					if countData[bankName] == nil {
						countData[bankName] = make(map[string]int)
					}
					countData[bankName][transactionType]++
				}
			}
		}

		return true
	})

	return countData
}

func IsDateInRange(date, startDate, endDate string) bool {
	if date == "" {
		//fmt.Println("Date string is empty")
		return false
	}

	parsedDate, err := time.Parse(time.RFC3339, date)
	if err != nil {
		fmt.Printf("Error parsing the date: %v\n", err)
		return false
	}

	parsedStartDate, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		fmt.Printf("Error parsing the start date: %v\n", err)
		return false
	}

	var parsedEndDate time.Time
	if endDate == "" {
		parsedEndDate = time.Now()
	} else {
		parsedEndDate, err = time.Parse("2006-01-02", endDate)
		if err != nil {
			fmt.Printf("Error parsing the end date: %v\n", err)
			return false
		}
	}

	parsedEndDate = parsedEndDate.Add(24 * time.Hour).Add(-time.Nanosecond)

	return parsedDate.After(parsedStartDate) && parsedDate.Before(parsedEndDate)
}
