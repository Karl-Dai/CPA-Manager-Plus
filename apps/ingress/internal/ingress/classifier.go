package ingress

// Route identifies the internal owner of a public request.
type Route uint8

const (
	RouteGateway Route = iota
	RouteManager
)

// ControlMatcher is the single replacement seam for control route ownership.
// Runtime 12 supplies transitional path evidence. A later configuration slice
// can replace it with an effective controlBasePath matcher without changing the
// transport proxies or Compose topology.
type ControlMatcher interface {
	Matches(path string) bool
}

type ControlMatcherFunc func(path string) bool

func (f ControlMatcherFunc) Matches(path string) bool {
	return f(path)
}

type Classifier struct {
	control ControlMatcher
}

func NewClassifier(control ControlMatcher) Classifier {
	if control == nil {
		panic("ingress: control matcher is required")
	}
	return Classifier{control: control}
}

func (c Classifier) Classify(path string) Route {
	if c.control.Matches(path) {
		return RouteManager
	}
	return RouteGateway
}

// NewTransitionalClassifier reflects only the Manager paths proven by the
// current v2 router. These paths are deployment compatibility evidence, not
// the long-term CPAMP public contract.
func NewTransitionalClassifier() Classifier {
	exact := map[string]struct{}{
		"/":                {},
		"/health":          {},
		"/management.html": {},
		"/setup":           {},
		"/status":          {},
	}
	prefixes := []string{
		"/usage-service",
		"/v0/management",
	}
	return NewClassifier(ControlMatcherFunc(func(path string) bool {
		if _, ok := exact[path]; ok {
			return true
		}
		for _, prefix := range prefixes {
			if path == prefix || len(path) > len(prefix) && path[:len(prefix)] == prefix && path[len(prefix)] == '/' {
				return true
			}
		}
		return false
	}))
}
