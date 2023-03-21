package cos418_hw1_1

import (
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
//
//	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
//
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuation and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"

	// Read file contents in bytes from specified path
	fileTextinBytes, errorInFileRead := ioutil.ReadFile(path)

	if errorInFileRead != nil {
		log.Fatal(errorInFileRead) //logs Error followed by exit call
	}

	// Converting fileText in bytes to list of lower case tokenized strings
	var listOfStrings []string = strings.Fields(strings.ToLower(string(fileTextinBytes)))

	filteredStringList := []string{}

	alphaNumericsRegex, errorInRegexCompile := regexp.Compile("[^a-zA-Z0-9]+")

	if errorInRegexCompile != nil {
		log.Fatal(errorInRegexCompile)
	}

	for index := range listOfStrings {
		// Replace Non alhpaNumeric charachters with empty string and filter text based on charThreshold
		listOfStrings[index] = alphaNumericsRegex.ReplaceAllString(listOfStrings[index], "")
		if len(listOfStrings[index]) >= charThreshold {
			filteredStringList = append(filteredStringList, listOfStrings[index])
		}
	}

	// Create a map of word and its count
	wordCountmap := make(map[string]int)
	for _, word := range filteredStringList {
		wordCountmap[word] += 1
	}

	topKwordCount := []WordCount{}

	for word := range wordCountmap {
		topKwordCount = append(topKwordCount, WordCount{Word: word, Count: wordCountmap[word]})
	}

	sortWordCounts(topKwordCount)
	return topKwordCount[0:numWords] //Slice the array to return top most K words
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
