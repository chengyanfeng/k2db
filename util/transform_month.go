package util

import "strings"

func transform(s string) int{
	var m int
	if(strings.EqualFold(s, "January")){
		m = 1
	}
	if(strings.EqualFold(s, "February")){
		m = 2
	}
	if(strings.EqualFold(s, "March")){
		m = 3
	}
	if(strings.EqualFold(s, "April")){
		m = 4
	}
	if(strings.EqualFold(s, "May")){
		m = 5
	}
	if(strings.EqualFold(s, "June")){
		m = 6
	}
	if(strings.EqualFold(s, "July")){
		m = 7
	}
	if(strings.EqualFold(s, "August")){
		m = 8
	}
	if(strings.EqualFold(s, "September")){
		m = 9
	}
	if(strings.EqualFold(s, "October")){
		m = 10
	}
	if(strings.EqualFold(s, "November")){
		m = 11
	}
	if(strings.EqualFold(s, "December")){
		m = 12
	}
	return m
}
