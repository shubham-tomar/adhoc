package main

import "fmt"

// This Covers GoLang Book chapter 2

func runP2() {
	// ===========================================
	// PART 1: Zero Values - Go's "Default Schema"
	// ===========================================
	// Unlike Python where uninitialized vars don't exist,
	// Go gives every type a sensible default (zero value).
	// Think of it like a table column with DEFAULT constraints.

	var count int        // zero value: 0
	var price float64    // zero value: 0.0
	var name string      // zero value: "" (empty string)
	var isActive bool    // zero value: false

	fmt.Println("=== Zero Values (Go's Default Schema) ===")
	fmt.Printf("int: %d, float64: %f, string: %q, bool: %t\n",
		count, price, name, isActive)

// | Symbol | Purpose                 |
// | ------ | ----------------------- |
// | `%d`   | Decimal integer         |
// | `%f`   | Float  %.1f,%.3f,%.0f for places  |
// | `%s`   | String                  |
// | `%q`   | Quoted string           |
// | `%t`   | Boolean                 |
// | `%v`   | Default value           |
// | `%T`   | Type                    |
// | `%+v`  | Struct with field names |
// | `%#v`  | Go syntax               |
// | `%x`   | Hex                     |

	// ===========================================
	// PART 2: Variable Declaration Styles
	// ===========================================
	// var  -> like declaring a column in DDL (explicit)
	// :=   -> like Python assignment (concise, inferred)

	// Style 1: var with explicit type (use when you want zero value)
	var maxRetries int = 3

	// Style 2: var with type inference
	var timeout = 30.5 // Go infers float64

	// Style 3: Short declaration := (most common inside functions)
	// This is like Python's simple assignment
	batchSize := 1000
	tableName := "ad_impressions"

	fmt.Println("\n=== Declaration Styles ===")
	fmt.Printf("maxRetries: %d, timeout: %.1f\n", maxRetries, timeout)
	fmt.Printf("batchSize: %d, tableName: %s\n", batchSize, tableName)

	// ===========================================
	// PART 3: Strict Type Conversion
	// ===========================================
	// In Python: 5 + 3.14 just works
	// In Go: You must be explicit - like CAST() in SQL

	var impressions int = 1000000
	var ctr float64 = 0.025

	// This would NOT compile:
	// clicks := impressions * ctr  // ERROR: mismatched types

	// You must explicitly convert (like SQL CAST):
	clicks := float64(impressions) * ctr
	fmt.Println("\n=== Strict Type Conversion ===")
	fmt.Printf("Impressions: %d, CTR: %.3f, Clicks: %.0f\n",
		impressions, ctr, clicks)

	// Even same "family" types need conversion:
	var smallNum int32 = 100
	var bigNum int64 = 999999999

	// sum := smallNum + bigNum  // ERROR: cannot mix int32 and int64
	sum := int64(smallNum) + bigNum // Must convert explicitly
	fmt.Printf("Sum of int32 + int64: %d\n", sum)

	// ===========================================
	// PART 4: Literals are Untyped (Flexible)
	// ===========================================
	// Literal numbers adapt to their context
	// Like how '42' in JSON can become int or string

	var a int = 42       // 42 becomes int
	var b float64 = 42   // 42 becomes float64
	var c int64 = 42     // 42 becomes int64

	fmt.Println("\n=== Untyped Literals ===")
	fmt.Printf("Same literal 42 as: int=%d, float64=%.1f, int64=%d\n", a, b, c)

	// ===========================================
	// PART 5: Constants
	// ===========================================
	// Immutable values known at compile time
	// Like environment configs that never change

	const maxBatchSize = 10000
	const kafkaTopic = "ad_events"
	const compressionRatio = 0.75

	fmt.Println("\n=== Constants ===")
	fmt.Printf("Topic: %s, MaxBatch: %d, Compression: %.2f\n",
		kafkaTopic, maxBatchSize, compressionRatio)

	// ===========================================
	// EXERCISE: Fix the Bug!
	// ===========================================
	// Uncomment and fix this code:

	var revenue int64 = 50000
	var conversionRate float32 = 0.12
	profit := float32(revenue) * conversionRate  // This won't compile!

	// Your fix here:
// 	var revenue int64 = 50000
// 	var conversionRate float32 = 0.12
// 	profit := float32(revenue) * conversionRate
	fmt.Println("\n=== Exercise Solution ===")
	fmt.Printf("Profit: %.2f\n", profit)

// 1.	 Write a program that declares an integer variable called i with the value 20.
// Assign i to a floating-point variable named f. Print out i and f. 
var i = 20
var f = float64(i)
fmt.Println(i)
fmt.Println(f)

// 2. Write a program that declares a constant called value that can be assigned to
// both an integer and a floating-point variable. Assign it to an integer called i and a
// floating-point variable called f. Print out i and f.
const value = 10
var i2 = value
var f2 float64 = value
fmt.Println(i2)
fmt.Println(f2)

// 3. Write a program with three variables, one named b of type byte, one named
// smallI of type int32, and one named bigI of type uint64. Assign each variable
// the maximum legal value for its type; then add 1 to each variable. Print out their
// values.
	var bi byte = 255
	var smallI int32 = 2147483647
	var bigI uint64 = 18446744073709551615

	bi = bi + 1
	smallI = smallI + 1
	bigI = bigI + 1

	fmt.Println(bi)
	fmt.Println(smallI)
	fmt.Println(bigI)

}