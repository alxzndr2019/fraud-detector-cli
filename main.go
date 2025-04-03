package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olekukonko/tablewriter"
)

// Transaction represents a financial transaction
type Transaction struct {
	ID        string    `json:"id"`
	Amount    float64   `json:"amount"`
	Timestamp time.Time `json:"timestamp"`
	AccountID string    `json:"account_id"`
	Merchant  string    `json:"merchant"`
}

// FraudResult represents a detected fraudulent transaction with reason
type FraudResult struct {
	Transaction Transaction
	Reason      string
}

// Config holds the fraud detection thresholds
type Config struct {
	HighAmountThreshold float64
	TimeWindow          time.Duration
	OutputFile          string
}

func main() {
	// Parse command line flags
	inputFile := flag.String("input", "transactions.csv", "Path to input file (CSV or JSON)")
	fileType := flag.String("type", "csv", "Input file type (csv or json)")
	highAmount := flag.Float64("amount", 1000.0, "High amount threshold")
	timeWindow := flag.Int("window", 5, "Time window in minutes for rapid transactions")
	outputFile := flag.String("output", "", "Output file for flagged transactions")
	
	flag.Parse()

	config := Config{
		HighAmountThreshold: *highAmount,
		TimeWindow:          time.Duration(*timeWindow) * time.Minute,
		OutputFile:          *outputFile,
	}

	// Read and parse transactions
	transactions, err := readTransactions(*inputFile, *fileType)
	if err != nil {
		fmt.Printf("Error reading transactions: %v\n", err)
		os.Exit(1)
	}

	// Detect fraudulent transactions
	fraudResults := detectFraud(transactions, config)

	// Display results
	displayResults(fraudResults)

	// Export results if output file specified
	if config.OutputFile != "" {
		err := exportResults(fraudResults, config.OutputFile)
		if err != nil {
			fmt.Printf("Error exporting results: %v\n", err)
		} else {
			fmt.Printf("\nResults exported to %s\n", config.OutputFile)
		}
	}
}

// readTransactions reads transactions from a file based on its type
func readTransactions(filePath, fileType string) ([]Transaction, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	switch strings.ToLower(fileType) {
	case "csv":
		return readCSV(file)
	case "json":
		return readJSON(file)
	default:
		return nil, fmt.Errorf("unsupported file type: %s", fileType)
	}
}

// readCSV reads transactions from a CSV file
func readCSV(file io.Reader) ([]Transaction, error) {
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var transactions []Transaction
	for i, record := range records {
		// Skip header
		if i == 0 {
			continue
		}

		if len(record) < 5 {
			return nil, fmt.Errorf("invalid CSV format at line %d", i+1)
		}

		amount, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			return nil, fmt.Errorf("invalid amount at line %d: %v", i+1, err)
		}

		timestamp, err := time.Parse(time.RFC3339, record[2])
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp at line %d: %v", i+1, err)
		}

		transactions = append(transactions, Transaction{
			ID:        record[0],
			Amount:    amount,
			Timestamp: timestamp,
			AccountID: record[3],
			Merchant:  record[4],
		})
	}

	return transactions, nil
}

// readJSON reads transactions from a JSON file
func readJSON(file io.Reader) ([]Transaction, error) {
	var transactions []Transaction
	decoder := json.NewDecoder(file)
	err := decoder.Decode(&transactions)
	if err != nil {
		return nil, err
	}
	return transactions, nil
}

// detectFraud applies fraud detection rules to transactions
func detectFraud(transactions []Transaction, config Config) []FraudResult {
	var results []FraudResult
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Process transactions in batches using goroutines
	batchSize := 100
	batches := len(transactions) / batchSize
	if len(transactions)%batchSize != 0 {
		batches++
	}

	for i := 0; i < batches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > len(transactions) {
			end = len(transactions)
		}

		wg.Add(1)
		go func(batch []Transaction) {
			defer wg.Done()
			batchResults := processBatch(batch, config)
			
			mu.Lock()
			results = append(results, batchResults...)
			mu.Unlock()
		}(transactions[start:end])
	}

	wg.Wait()

	return results
}

// processBatch processes a batch of transactions for fraud detection
func processBatch(batch []Transaction, config Config) []FraudResult {
	var batchResults []FraudResult

	for i, tx := range batch {
		// Rule 1: High amount
		if tx.Amount > config.HighAmountThreshold {
			batchResults = append(batchResults, FraudResult{
				Transaction: tx,
				Reason:      fmt.Sprintf("High amount: $%.2f", tx.Amount),
			})
		}

		// Rule 2: Rapid succession (check next transactions in the batch)
		for j := i + 1; j < len(batch); j++ {
			nextTx := batch[j]
			if nextTx.AccountID != tx.AccountID {
				continue
			}

			timeDiff := nextTx.Timestamp.Sub(tx.Timestamp)
			if timeDiff < config.TimeWindow && timeDiff > 0 {
				batchResults = append(batchResults, FraudResult{
					Transaction: tx,
					Reason:      fmt.Sprintf("Rapid transaction: %v later with $%.2f", timeDiff, nextTx.Amount),
				})
				batchResults = append(batchResults, FraudResult{
					Transaction: nextTx,
					Reason:      fmt.Sprintf("Rapid transaction: following $%.2f after %v", tx.Amount, timeDiff),
				})
			}
		}
	}

	return batchResults
}

// displayResults shows the fraud results in a table format
func displayResults(results []FraudResult) {
	if len(results) == 0 {
		fmt.Println("No fraudulent transactions detected.")
		return
	}

	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Account", "Merchant", "Amount", "Timestamp", "Reason"})
	table.SetBorder(false)
	table.SetRowLine(true)

	for _, result := range results {
		tx := result.Transaction
		table.Append([]string{
			tx.ID,
			tx.AccountID,
			tx.Merchant,
			fmt.Sprintf("$%.2f", tx.Amount),
			tx.Timestamp.Format(time.RFC3339),
			result.Reason,
		})
	}

	fmt.Println("Potentially Fraudulent Transactions:")
	table.Render()
}

// exportResults writes the fraud results to a file
func exportResults(results []FraudResult, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}