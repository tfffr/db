package db

import (
	"database/sql"
	"testing"
)

func TestAIResponseCache(t *testing.T) {
	conn := SetupTestDB(t)
	defer conn.Close()

	hash := "a1b2c3d4e5f6g7h8i9j0"
	responseJSON := []byte(`{"summary": "test response", "tokens_used": 42}`)

	// test insert
	err := conn.SaveAIResponse(hash, responseJSON)
	if err != nil {
		t.Fatalf("failed to save AI response: %v", err)
	}

	// test get
	savedResponse, err := conn.GetAIResponse(hash)
	if err != nil {
		t.Fatalf("failed to get AI response: %v", err)
	}

	// test data equal
	if string(savedResponse) != string(responseJSON) {
		t.Errorf("%s does not match with %s", string(savedResponse), string(responseJSON))
	}

	// test update
	updatedResponseJSON := []byte(`{"summary": "updated response", "tokens_used": 100}`)
	err = conn.SaveAIResponse(hash, updatedResponseJSON)
	if err != nil {
		t.Fatalf("failed to update AI response: %v", err)
	}

	// test read update
	savedResponse, err = conn.GetAIResponse(hash)
	if err != nil {
		t.Fatalf("failed to get AI response: %v", err)
	}

	// test updated data equal
	if string(savedResponse) != string(updatedResponseJSON) {
		t.Errorf("%s does not match with updated %s", string(savedResponse), string(responseJSON))
	}

	// test sql.ErrNoRows
	_, err = conn.GetAIResponse("non_existent_hash")
	if err == nil {
		t.Errorf("failed to get error sql.ErrNoRows")
	} else if err != sql.ErrNoRows {
		t.Errorf("error != sql.ErrNoRows: %v", err)
	}
}
