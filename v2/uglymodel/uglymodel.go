package uglymodel

type UglyModel struct {
	ID        string `json:"id" dynamodbav:",omitempty"`
	UglyGroup string `json:"-" dynamodbav:",omitempty"`
	UglyId    string `json:"-" dynamodbav:",omitempty"`
}
