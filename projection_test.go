package dynago

import "testing"

func Test_buildProjection(t *testing.T) {
	type args struct {
		schema interface{}
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"flat schema",
			args{struct {
				Name string
				Age  int
			}{}},
			"Name,Age,",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildProjection(tt.args.schema); got != tt.want {
				t.Errorf("buildProjection() = %v, want %v", got, tt.want)
			}
		})
	}
}
