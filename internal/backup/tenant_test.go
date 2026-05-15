package backup

import "testing"

func TestParseCollectionSpecs(t *testing.T) {
	got, err := ParseCollectionSpecs(" app.users , orders.items ")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].DB != "app" || got[0].Coll != "users" || got[1].DB != "orders" || got[1].Coll != "items" {
		t.Fatalf("unexpected %+v", got)
	}
}

func TestSanitizeTenantPath(t *testing.T) {
	s, err := SanitizeTenantPath("acme/corp")
	if err != nil || s != "acme_corp" {
		t.Fatalf("%q %v", s, err)
	}
}
