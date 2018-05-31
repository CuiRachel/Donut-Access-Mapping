# Donut-Accessibility-Mapping

This tool is written in python used for mapping donut accessibility by auto for the Minneapolis - St.Paul Metropolitan region specifically (The future version of this tool is expected to allow donut accessibility mappings for other areas). The tool is named as "MappingDonutAccess.py"

Input: GeoID of a census block

Output: A shapefile showing the number of reachable destinations for the selected block in different time ranges
## USAGE

MappingDonutAccess.py -g $<$geoid$>$ -s $<$schema$>$ -p $<$input\_path$>$ -f $<$input\_file$>$ -o $<$output\_path$>$ -d $<$dbname$>$ -u $<$user$>$

### Option Descriptions:

 -g <geoid>: GeoID of the selected block
  
 -s <schema>: Defined database schema for output storage
  
 -p <input\_path>: File path of travel time matrix table or other required input materials
 
 -f <input\_file>: Name of travel time matrix table
 
 -o <output\_path>: File path of output shapefile
 
 -d <dbname>: dbname for database connection
  
 -u <user>: username for database connection
  
 -f <input\_file> is not mandatory. But all the other options need to be defined for running the tool.

The flow chart of donut accessibility mapping tool is shown in Figure

[MappingFlowChart.pdf](https://github.umn.edu/AccessibilityObservatory/Donut-Accessibility-Mapping/files/425/MappingFlowChart.pdf)


### Other Required Data Stored in Input Path:

    • Census tiger block data named ”Census2010TigerBlock.shp”

    • Centroid of census block data named ”blockswac.shp” (required if AOBatchAnalyst needs to run )

    • AOBatchAnalyst tools named ”AOBatchanalyst-0.2.3-all.jar” (required if AOBatchAnalyst needs to run)

## Examples of Donut Accessibility Maps Based on the Tool

[DonutAccessDowntown.pdf](https://github.umn.edu/AccessibilityObservatory/Donut-Accessibility-Mapping/files/426/DonutAccessDowntownBlockFixedCleaned.pdf)

[DonutAccessSuburban.pdf](https://github.umn.edu/AccessibilityObservatory/Donut-Accessibility-Mapping/files/427/DonutAccessSuburbanBlockFixedCleaned.pdf)

[DonutAccessExurban.pdf](https://github.umn.edu/AccessibilityObservatory/Donut-Accessibility-Mapping/files/428/DonutAccessExurbanBlockFixedCleaned.pdf)
