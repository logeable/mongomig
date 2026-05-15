package backup

import "go.mongodb.org/mongo-driver/bson/primitive"

func tsAfter(a, b primitive.Timestamp) bool {
	if a.T != b.T {
		return a.T > b.T
	}
	return a.I > b.I
}
