package provision

import (
	"context"
	"crypto/rand"
	_ "embed"
	"errors"
)

type AccessScope struct {
	Groups    []string
	Resources []string
}

type Provisioner interface {
	Connect(string) error
	Close() error
	DropUser(context.Context, string) error
	UserExists(context.Context, string) (*bool, error)
	CreateUser(context.Context, string, string) error
	GrantReadOnly(context.Context, string, AccessScope) error
}

func GeneratePassword() (string, error) {
	const (
		length     = 20
		maxRetries = 20
		chars      = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*()-_=+"
	)

	for i := 0; i < maxRetries; i++ {
		b := make([]byte, length)
		if _, err := rand.Read(b); err != nil {
			return "", err
		}

		var hasLower, hasUpper, hasDigit, hasSymbol bool

		for i := range b {
			c := chars[int(b[i])%len(chars)]
			b[i] = c

			switch {
			case 'a' <= c && c <= 'z':
				hasLower = true
			case 'A' <= c && c <= 'Z':
				hasUpper = true
			case '0' <= c && c <= '9':
				hasDigit = true
			default:
				hasSymbol = true
			}
		}

		if hasLower && hasUpper && hasDigit && hasSymbol {
			return string(b), nil
		}
	}

	return "", errors.New("could not generate password after 20 attempts")
}
