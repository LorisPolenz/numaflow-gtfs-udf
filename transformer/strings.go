package transformer

import (
	"fmt"
	"strings"
)

type ToUpper struct {
	Input  string
	Output string
}

type ToLower struct {
	Input  string
	Output string
}

type Split struct {
	Input     string
	Delimiter string
	Parts     []string
}

type Join struct {
	Parts     []string
	Delimiter string
	Output    string
}

// Contruct type
func NewToUpper(input string) *ToUpper {
	return &ToUpper{Input: input}
}

func NewToLower(input string) *ToLower {
	return &ToLower{Input: input}
}

func NewSplit(input, delimiter string) *Split {
	return &Split{Input: input, Delimiter: delimiter}
}

func NewJoin(parts []string, delimiter string) *Join {
	return &Join{Parts: parts, Delimiter: delimiter}
}

// Func implementing Transformer interface, depending on the type the right func is used
func (t *ToUpper) Transform() {
	fmt.Println("Transforming to Uppercase:", t.Input)
	t.Output = strings.ToUpper(t.Input)
	fmt.Println("Transforming to Uppercase:", t.Output)
}
func (t *ToLower) Transform() {
	t.Output = strings.ToLower(t.Input)
}
func (s *Split) Transform() {
	s.Parts = strings.Split(s.Input, s.Delimiter)
}
func (j *Join) Transform() {
	j.Output = strings.Join(j.Parts, j.Delimiter)
}
