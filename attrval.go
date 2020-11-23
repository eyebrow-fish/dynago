package dynago

import (
	"fmt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"strconv"
)

func buildValue(av *dynamodb.AttributeValue) (interface{}, error) {
	if av.S != nil {
		return *av.S, nil
	} else if av.BOOL != nil {
		return *av.BOOL, nil
	} else if av.B != nil {
		return av.B, nil
	} else if av.BS != nil {
		return av.BS, nil
	} else if av.SS != nil {
		var ss []string
		for _, v := range av.SS {
			ss = append(ss, *v)
		}
		return ss, nil
	} else if av.N != nil {
		n, err := strconv.Atoi(*av.N)
		if err != nil {
			return nil, err
		}
		return n, nil
	} else if av.NS != nil {
		var ns []int
		for _, v := range av.NS {
			n, err := strconv.Atoi(*v)
			if err != nil {
				return nil, err
			}
			ns = append(ns, n)
		}
		return ns, nil
	} else if av.L != nil {
		var l []interface{}
		for _, v := range av.L {
			val, err := buildValue(v)
			if err != nil {
				return nil, err
			}
			l = append(l, val)
		}
		return l, nil
	} else if av.M != nil {
		m := make(map[string]interface{})
		for k, v := range av.M {
			val, err := buildValue(v)
			if err != nil {
				return nil, err
			}
			m[k] = val
		}
		return m, nil
	}
	return nil, fmt.Errorf("could not build value %v", av)
}
