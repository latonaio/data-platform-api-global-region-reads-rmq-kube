package requests

type GlobalRegion struct {
	GlobalRegion		string	`json:"GlobalRegion"`
	CreationDate		string	`json:"CreationDate"`
	LastChangeDate		string	`json:"LastChangeDate"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
