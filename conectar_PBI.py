import pandas as pd
import env

from sys import path
from pyadomd import Pyadomd


port_number='localhost:65373'

conn_str = f'Provider=MSOLAP;Persist Security Info=True;Data Source={port_number};Catalog={env.model_name};'


dax_query="""
// DAX Query
DEFINE
	VAR __DS0Core = 
		SUMMARIZECOLUMNS(
			'Events'[id],
			'Events'[component],
			'Events'[name],
			'Events'[QueryText],
			"SumDurationMs", CALCULATE(SUM('Events'[DurationMs]))
		)

	VAR __DS0PrimaryWindowed = 
		TOPN(
			501,
			__DS0Core,
			'Events'[id],
			1,
			'Events'[component],
			1,
			'Events'[name],
			1,
			'Events'[QueryText],
			1
		)

EVALUATE
	__DS0PrimaryWindowed

ORDER BY
	'Events'[id], 'Events'[component], 'Events'[name], 'Events'[QueryText]

"""
 
con = Pyadomd(conn_str)

con.open()

result = con.cursor().execute(dax_query)
df = pd.DataFrame(result.fetchone())


print(df)


con.close()