package uel

import (
	"errors"
	"fmt"
)

var (
	ErrParser     = errors.New("parser error")
	ErrUnboundVar = errors.New("unbound variable")
)

const (
	identChars                   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_0123456789"
	disallowedStartingIdentChars = "0123456789."
	numberChars                  = "0123456789_."
)

type Parser struct {
	source         []rune
	pos, line, col int
	currentChar    rune
}

func NewParser(source string) *Parser {
	p := &Parser{
		source:      []rune(source),
		pos:         0,
		line:        1,
		col:         1,
		currentChar: -1,
	}
	if len(p.source) > 0 {
		p.currentChar = p.source[0]
	}
	return p
}

func (p *Parser) advance(distance int) error {
	for i := 0; i < distance; i++ {
		if p.eof() {
			return errors.New("unexpected eof")
		}
		if p.currentChar == '\n' {
			p.line++
			p.col = 1
		} else {
			p.col++
		}
		p.pos++
		if p.pos >= len(p.source) {
			p.currentChar = -1
		} else {
			p.currentChar = p.source[p.pos]
		}
	}
	return nil
}

func (p *Parser) checkpoint() (pos, col, line int) {
	return p.pos, p.col, p.line
}

func (p *Parser) restore(pos, col, line int) {
	p.pos, p.col, p.line = pos, col, line
	if p.pos >= len(p.source) {
		p.currentChar = -1
	} else {
		p.currentChar = p.source[p.pos]
	}
}

func (p *Parser) sourceRef(pos, col, line int) (_line, _col int) {
	return line, col
}

func (p *Parser) sourceError(message string) error {
	return fmt.Errorf("%w: line %d, column %d: %q", ErrParser, p.line, p.col, message)
}

func (p *Parser) eof() bool {
	return p.pos >= len(p.source)
}

func (p *Parser) char(lookahead int) rune {
	if p.pos+lookahead >= len(p.source) || p.pos+lookahead < 0 {
		return -1
	}
	return p.source[p.pos+lookahead]
}

func (p *Parser) string(width int) string {
	return string(p.source[p.pos:][:width])
}

func (p *Parser) skipComment() (bool, error) {
	if p.currentChar != '#' {
		return false, nil
	}
	err := p.advance(1)
	if err != nil {
		return false, err
	}
	for {
		if p.eof() {
			return true, nil
		}
		err = p.advance(1)
		if err != nil {
			return false, err
		}
		if p.currentChar == '\n' {
			return true, nil
		}
	}
}

func (p *Parser) skipWhitespace() (bool, error) {
	if p.eof() {
		return false, nil
	}
	skipped, err := p.skipComment()
	if err != nil {
		return false, err
	}
	if skipped {
		return true, nil
	}
	switch p.currentChar {
	case ' ', '\t', '\r', '\n':
		return true, p.advance(1)
	}
	return false, nil
}

func (p *Parser) skipAllWhitespace() (bool, error) {
	anySkipped := false
	for {
		skipped, err := p.skipWhitespace()
		if err != nil {
			return false, err
		}
		if !skipped {
			return anySkipped, nil
		}
		anySkipped = true
	}
}

func (p *Parser) parseIdentifier