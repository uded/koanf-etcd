package etcd

import (
	"bytes"
	"fmt"
	"net"
	"strings"
)

// makeDefaultKeyTransform returns the default key-to-koanf-path function:
//
//   - In prefix mode, strip the prefix.
//   - Replace any "/" in the (possibly trimmed) key with the delimiter.
//
// Callers can override via WithKeyTransform.
func makeDefaultKeyTransform(s *settings) func(string) string {
	prefix := s.prefix
	delim := s.delim
	trim := s.trimPrefix && prefix != ""
	return func(k string) string {
		if trim {
			k = strings.TrimPrefix(k, prefix)
		}
		if delim != "/" {
			k = strings.ReplaceAll(k, "/", delim)
		}
		return k
	}
}

// defaultValueTransform trims surrounding whitespace from the raw value
// and returns it as a string. Etcdctl and CI pipelines frequently
// introduce trailing newlines that would otherwise produce distinct
// values for "http://x" vs "http://x\n".
func defaultValueTransform(_ string, raw []byte) (any, error) {
	return string(bytes.TrimSpace(raw)), nil
}

// lookupSRVEndpoints resolves an etcd SRV record (e.g.
// _etcd-client._tcp.example.com) and returns endpoints formatted as
// host:port.
func lookupSRVEndpoints(service, proto, domain string) ([]string, error) {
	_, addrs, err := net.LookupSRV(service, proto, domain)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, fmt.Sprintf("%s:%d", strings.TrimSuffix(a.Target, "."), a.Port))
	}
	return out, nil
}
