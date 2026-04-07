package engine

// KeyRangeMidpoint computes the midpoint of the [start, end) key range.
// Used to determine the split key for Actors that do not implement SplitHinter.
func KeyRangeMidpoint(start, end string) string {
	if start == "" && end == "" {
		return "m"
	}
	if end == "" {
		return start + "m"
	}
	sb := []byte(start)
	eb := []byte(end)
	maxLen := len(eb)
	if len(sb) > maxLen {
		maxLen = len(sb)
	}
	for len(sb) < maxLen {
		sb = append(sb, 0)
	}
	for len(eb) < maxLen {
		eb = append(eb, 0)
	}
	result := make([]byte, maxLen)
	carry := 0
	for i := maxLen - 1; i >= 0; i-- {
		sum := int(sb[i]) + int(eb[i]) + carry*256
		result[i] = byte(sum / 2)
		carry = sum % 2
	}
	n := len(result)
	for n > 0 && result[n-1] == 0 {
		n--
	}
	if n == 0 {
		n = 1
	}
	mid := string(result[:n])
	if mid <= start {
		return start + "m"
	}
	return mid
}
