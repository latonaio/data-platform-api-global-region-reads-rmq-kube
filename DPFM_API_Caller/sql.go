package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-global-region-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-global-region-reads-rmq-kube/DPFM_API_Output_Formatter"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var globalRegion *[]dpfm_api_output_formatter.GlobalRegion
	var globalRegionText *[]dpfm_api_output_formatter.GlobalRegionText
	for _, fn := range accepter {
		switch fn {
		case "GlobalRegion":
			func() {
				globalRegion = c.GlobalRegion(mtx, input, output, errs, log)
			}()
		case "GlobalRegions":
			func() {
				globalRegion = c.GlobalRegions(mtx, input, output, errs, log)
			}()
		case "GlobalRegionText":
			func() {
				globalRegionText = c.GlobalRegionText(mtx, input, output, errs, log)
			}()
		case "GlobalRegionTexts":
			func() {
				globalRegionText = c.GlobalRegionTexts(mtx, input, output, errs, log)
			}()
		default:
		}
	}

	data := &dpfm_api_output_formatter.Message{
		GlobalRegion:     globalRegion,
		GlobalRegionText: globalRegionText,
	}

	return data
}

func (c *DPFMAPICaller) GlobalRegion(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.GlobalRegion {
	where := fmt.Sprintf("WHERE GlobalRegion = '%s'", input.GlobalRegion.GlobalRegion)

	if input.GlobalRegion.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.GlobalRegion.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_global_region_global_region_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, GlobalRegion DESC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGlobalRegion(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) GlobalRegions(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.GlobalRegion {

	if input.GlobalRegion.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.GlobalRegion.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_global_region_global_region_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, GlobalRegion DESC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGlobalRegion(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) GlobalRegionText(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.GlobalRegionText {
	var args []interface{}
	globalRegion := input.GlobalRegion.GlobalRegion
	globalRegionText := input.GlobalRegion.GlobalRegionText

	cnt := 0
	for _, v := range globalRegionText {
		args = append(args, globalRegion, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?,?),", cnt-1) + "(?,?)"
	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_global_region_global_region_data
		WHERE (GlobalRegion, Language) IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToGlobalRegionText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) GlobalRegionTexts(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.GlobalRegionText {
	var args []interface{}
	globalRegionText := input.GlobalRegion.GlobalRegionText

	cnt := 0
	for _, v := range globalRegionText {
		args = append(args, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?),", cnt-1) + "(?)"
	rows, err := c.db.Query(
		`SELECT * 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_global_region_global_region_data
		WHERE Language IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	//
	data, err := dpfm_api_output_formatter.ConvertToGlobalRegionText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
