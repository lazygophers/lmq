package state

func Incr(s string) string {
	if s == "" {
		s = "0"
	}

	// 字符串数字的 +1
	l := len(s) - 1
	last := s[l]

	if last == '9' {
		return Incr(s[:l]) + "0"

	}

	return s[:l] + string(last+1)
}

func Decr(s string) string {
	if s == "" {
		s = "0"
	}

	if s == "0" {
		return "0"
	}

	// 字符串数字的 -1
	l := len(s) - 1
	last := s[l]

	if last == '0' {
		s = Decr(s[:l]) + "9"

		if s[0] != '0' {
			return s
		}

		if len(s) == 1 {
			return s
		}
		return s[1:]
	}

	return s[:l] + string(last-1)
}

// Gt 大于
func Gt(a, b string) bool {
	if a == "" {
		a = "0"
	}

	if b == "" {
		b = "0"
	}

	if len(a) > len(b) {
		return true
	}

	if len(a) < len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] > b[i] {
			return true
		}

		if a[i] < b[i] {
			return false
		}
	}

	return false
}

// Gte 大于等于
func Gte(a, b string) bool {
	if a == "" {
		a = "0"
	}

	if b == "" {
		b = "0"
	}

	if len(a) > len(b) {
		return true
	}

	if len(a) < len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] > b[i] {
			return true
		}

		if a[i] < b[i] {
			return false
		}
	}

	return true
}

// Lt 小于
func Lt(a, b string) bool {
	if a == "" {
		a = "0"
	}

	if b == "" {
		b = "0"
	}

	if len(a) > len(b) {
		return false
	}

	if len(a) < len(b) {
		return true
	}

	for i := 0; i < len(a); i++ {
		if a[i] > b[i] {
			return false
		}

		if a[i] < b[i] {
			return true
		}
	}

	return false
}

// Lte 小于等于
func Lte(a, b string) bool {
	if a == "" {
		a = "0"
	}

	if b == "" {
		b = "0"
	}

	if len(a) > len(b) {
		return false
	}

	if len(a) < len(b) {
		return true
	}

	for i := 0; i < len(a); i++ {
		if a[i] > b[i] {
			return false
		}

		if a[i] < b[i] {
			return true
		}
	}

	return true
}

func IsNumber(s string) bool {
	if s == "" {
		return true
	}

	for _, v := range s {
		if v < '0' || v > '9' {
			return false
		}
	}

	return true
}
