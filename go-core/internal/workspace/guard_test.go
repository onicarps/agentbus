package workspace

import "testing"

func TestLooksLikeDrvFS(t *testing.T) {
	cases := []struct {
		path string
		bad  bool
	}{
		{"/home/oni/okf_agent_workspace", false},
		{"/tmp/ws", false},
		{"/mnt/c/Users/foo", true},
		{"/mnt/d/proj", true},
		{"/cygdrive/c/Users", true},
		{"C:/Users/oni", true},
	}
	for _, tc := range cases {
		got := looksLikeDrvFS(tc.path)
		if got != tc.bad {
			t.Errorf("looksLikeDrvFS(%q)=%v want %v", tc.path, got, tc.bad)
		}
	}
}

func TestAssertSupportedHome(t *testing.T) {
	abs, err := AssertSupported("/home/oni/okf_agent_workspace")
	if err != nil {
		t.Fatal(err)
	}
	if abs == "" {
		t.Fatal("empty abs")
	}
}

func TestAssertSupportedDrvFS(t *testing.T) {
	t.Setenv("AGENTBUS_ALLOW_DRVFS", "")
	_, err := AssertSupported("/mnt/c/Users/foo/project")
	if err == nil {
		t.Fatal("expected error for /mnt/c")
	}
	t.Setenv("AGENTBUS_ALLOW_DRVFS", "1")
	if _, err := AssertSupported("/mnt/c/Users/foo/project"); err != nil {
		t.Fatalf("break-glass should allow: %v", err)
	}
}
