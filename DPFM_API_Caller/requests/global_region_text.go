package requests

type GlobalRegionText struct {
	GlobalRegion     	string  `json:"GlobalRegion"`
	Language          	string  `json:"Language"`
	GlobalRegionName	string 	`json:"GlobalRegionName"`
	CreationDate		string	`json:"CreationDate"`
	LastChangeDate		string	`json:"LastChangeDate"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
