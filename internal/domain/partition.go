package domain

// KeyRange는 [Start, End) 형태의 반열린 구간으로 파티션이 담당하는 키 범위를 나타낸다.
// End가 빈 문자열("")이면 상한이 없음을 의미한다 (Start 이상 모든 키).
type KeyRange struct {
	Start string // inclusive
	End   string // exclusive; "" = 상한 없음
}

// Contains는 key가 이 KeyRange에 속하는지 반환한다.
func (r KeyRange) Contains(key string) bool {
	if key < r.Start {
		return false
	}
	if r.End == "" {
		return true
	}
	return key < r.End
}

// Overlaps는 두 KeyRange가 겹치는 영역이 있는지 반환한다.
// split/migration 전 범위 충돌 검증에 사용한다.
func (r KeyRange) Overlaps(other KeyRange) bool {
	// [r.Start, r.End) 와 [other.Start, other.End) 가 겹치려면:
	// r.Start < other.End && other.Start < r.End
	// 단, End == "" 는 +∞ 로 취급한다.
	if other.End != "" && r.Start >= other.End {
		return false
	}
	if r.End != "" && other.Start >= r.End {
		return false
	}
	return true
}

// Partition은 클러스터 내 하나의 파티션 단위.
// 유일한 ID, actor 타입, 담당 키 범위를 가진다.
// 키 범위 중복 검증은 동일 ActorType 내에서만 수행한다.
type Partition struct {
	ID        string
	ActorType string // actor type 식별자. 동일 ActorType 내에서 키 범위가 유일해야 한다.
	KeyRange  KeyRange
}
