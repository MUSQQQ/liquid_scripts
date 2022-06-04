package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/stripe/stripe-go/v72"
	"github.com/stripe/stripe-go/v72/price"
)

const (
	dbURL     = ``
	stripeKey = ""
	fileName  = ""
)

const (
	titleCol       = 1
	genreCol       = 2
	developerCol   = 3
	publisherCol   = 4
	coverCol       = 5
	stripeCol      = 6
	priceCol       = 7
	dateCol        = 8
	descriptionCol = 9
)

type Game struct {
	Title       string
	Genre       string
	Developer   string
	Publisher   string
	CoverURL    string
	StripeID    string
	Price       float64
	ReleaseDate time.Time
	Description string
}

type DBWrapper struct {
	db *sql.DB
}

func Connect() (*sql.DB, error) {
	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Panic(err)
	}

	return db, nil
}

func (db *DBWrapper) AddGame(game *Game) error {
	rawSql := `INSERT INTO games (title, genre, developer, publisher,
				release_date, cover_url, stripe_id, price, description)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`

	_, err := db.db.Exec(
		rawSql,
		game.Title,
		game.Genre,
		game.Developer,
		game.Publisher,
		game.ReleaseDate,
		game.CoverURL,
		game.StripeID,
		game.Price,
		game.Description,
	)

	return err
}

func CreatePrice(game *Game) (string, error) {
	params := &stripe.PriceParams{
		Currency:   stripe.String(string(stripe.CurrencyPLN)),
		UnitAmount: stripe.Int64(int64(game.Price * 100)),
		ProductData: &stripe.PriceProductDataParams{
			Name: stripe.String(game.Title),
		},
	}
	stripePrice, err := price.New(params)
	if err != nil {
		return "", err
	}

	return stripePrice.ID, nil
}

func ProcessLineByLine(db *DBWrapper, filename string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	csvReader := csv.NewReader(file)

	// first row contains column names
	_, err = csvReader.Read()
	if err == io.EOF {
		return
	}

	counter := 0

	for {
		counter += 1

		row, err := csvReader.Read()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Fatal(err)
		}

		priceFloat, err := strconv.ParseFloat(row[priceCol], 64)
		if err != nil {
			log.Printf("parsing nr %d:  %s", counter, row[priceCol])
			continue
		}

		date, error := time.Parse("2006-01-02", row[dateCol])
		if error != nil {
			log.Printf("parsing date %d: %s", counter, row[dateCol])
			continue
		}

		game := &Game{
			Title:       row[titleCol],
			Genre:       row[genreCol],
			Developer:   row[developerCol],
			Publisher:   row[publisherCol],
			CoverURL:    row[coverCol],
			Price:       priceFloat,
			ReleaseDate: date,
			Description: row[descriptionCol],
		}

		priceID, err := CreatePrice(game)
		if err != nil {
			log.Printf("stripe connect %d: %v", counter, err)
			continue
		}

		game.StripeID = priceID
		err = db.AddGame(game)
		if err != nil {
			log.Printf("db connect %d, %v", counter, err)
			continue
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func main() {
	stripe.Key = stripeKey
	fmt.Println("script starts")

	db, _ := Connect()

	dbWrapper := &DBWrapper{db}

	ProcessLineByLine(dbWrapper, fileName)

	fmt.Println("script ends")
}
