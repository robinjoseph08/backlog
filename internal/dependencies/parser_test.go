package dependencies

import (
	"reflect"
	"testing"
)

func TestParseKeepsOnlyExplicitDependencyStatements(t *testing.T) {
	t.Parallel()

	got := Parse([]string{
		"Related work: #2. Blocked by #10 and depends on octo/widgets#11.",
		"Waiting for https://github.com/acme/api/issues/12 before starting. See #13.",
	})
	want := []Reference{
		{Number: 10},
		{Owner: "octo", Repo: "widgets", Number: 11},
		{Owner: "acme", Repo: "api", Number: 12},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestParseRecognizesExplicitBlockerLists(t *testing.T) {
	t.Parallel()

	got := Parse([]string{
		"Blocked by #10, #11, and acme/api#12. Depends on https://github.com/octo/widgets/issues/13 and #14.",
	})
	want := []Reference{
		{Number: 10}, {Number: 11}, {Owner: "acme", Repo: "api", Number: 12},
		{Owner: "octo", Repo: "widgets", Number: 13}, {Number: 14},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestParseLaterRemovalSupersedesTextBlocker(t *testing.T) {
	t.Parallel()

	got := Parse([]string{
		"Blocked by #10. Depends on octo/widgets#11. Waiting for https://github.com/acme/api/issues/12.",
		"No longer blocked by #10; the dependency on octo/widgets#11 was removed.",
		"Blocker https://github.com/acme/api/issues/12 is resolved and no longer blocks this work.",
	})
	if len(got) != 0 {
		t.Fatalf("got %#v, want no active references", got)
	}
}

func TestParseCanonicalizesEquivalentLocalAndURLReferences(t *testing.T) {
	t.Parallel()

	got := ParseForRepository([]string{
		"Blocked by https://github.com/acme/widgets/issues/10.",
		"No longer blocked by #10.",
	}, "acme", "widgets")
	if len(got) != 0 {
		t.Fatalf("got %#v, want equivalent local removal to supersede URL", got)
	}
}

func TestParseCanReintroduceRemovedDependency(t *testing.T) {
	t.Parallel()

	got := Parse([]string{
		"Blocked by #10.",
		"No longer blocked by #10.",
		"Blocked by #10 again.",
	})
	want := []Reference{{Number: 10}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestParseDoesNotTreatChecklistOrBareURLAsDependency(t *testing.T) {
	t.Parallel()

	got := Parse([]string{
		"- [ ] #10\nSee https://github.com/acme/api/issues/12\nAfter the API cleanup, revisit this.",
	})
	if len(got) != 0 {
		t.Fatalf("got %#v, want no active references", got)
	}
}
