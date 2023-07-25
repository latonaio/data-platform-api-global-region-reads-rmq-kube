package dpfm_api_output_formatter

import (
	"data-platform-api-global-region-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToGlobalRegion(rows *sql.Rows) (*[]GlobalRegion, error) {
	defer rows.Close()
	globalRegion := make([]GlobalRegion, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.GlobalRegion{}

		err := rows.Scan(
			&pm.GlobalRegion,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &globalRegion, nil
		}

		data := pm
		globalRegion = append(globalRegion, GlobalRegion{
			GlobalRegion:			data.GlobalRegion,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &globalRegion, nil
}

func ConvertToGlobalRegionText(rows *sql.Rows) (*[]GlobalRegionText, error) {
	defer rows.Close()
	globalRegionText := make([]GlobalRegionText, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.GlobalRegionText{}

		err := rows.Scan(
			&pm.GlobalRegion,
			&pm.Language,
			&pm.GlobalRegionName,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &globalRegionText, err
		}

		data := pm
		globalRegionText = append(globalRegionText, GlobalRegionText{
			GlobalRegion:     		data.GlobalRegion,
			Language:          		data.Language,
			GlobalRegionName:		data.GlobalRegionName,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &globalRegionText, nil
}
