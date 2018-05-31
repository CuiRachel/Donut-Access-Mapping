
import csv
import os
import math
import sys
import getopt

from shapely.geometry import mapping, shape
from shapely.ops import cascaded_union
from fiona import collection
import fiona
import psycopg2 as pg
from dbfread import DBF
import numpy as np
import json
import subprocess



def main(argv):
   
   geoid=''
   schema = ''
   input_path=''
   input_file=''
   output_path = ''
   dbname=''
   user=''   
   try:
      opts, args = getopt.getopt(argv,"hg:s:p:f:o:d:u:",["geoid=","schema=","input_path=","input_file=","output_path=", "dbname=","user="])
   except getopt.GetoptError:
      print('MappingDonutAccess.py -g <geoid> -s <schema> -p <input_path> -f <input_file> -o <output_path> -d <dbname> -u <user>')
      print('Error: ')
      sys.exit(2)   
   for opt, arg in opts:
    
      if opt == '-h':
         print('MappingDonutAccess.py -g <geoid> -s <schema> -p <input_path> -f <input_file> -o <output_path> -d <dbname> -u <user>')
         sys.exit()
      elif opt in ("-g", "--geoid"):
         geoid = arg
      elif opt in ("-s", "--schema"):
         database_schema = arg
      elif opt in ("-p", "--input_path"):
         input_path = arg
      elif opt in ("-f", "--input_file"):
         input_file = arg
      elif opt in ("-o", "--output_path"):
         output_path = arg
      elif opt in ("-d", "--dbname"):
         dbname = arg
      elif opt in ("-u", "--user"):
         user = arg
   

if __name__ == "__main__":
   main(sys.argv[1:])

argument_list=sys.argv
origin=''
schema=''
input_path=''
input_file=''
output_path=''
dbname=''
user=''

for i, value in enumerate (argument_list):
    print(value)
    if value=='-g':
        origin=argument_list[i+1]
    if value=='-s':
        schema=argument_list[i+1]
    if value=='-p':
        input_path=argument_list[i+1]
    if value=='-f':
        input_file=argument_list[i+1]
    if value=='-o':
        output_path=argument_list[i+1]
    if value=='-d':
        dbname=argument_list[i+1]
    if value=='-u':
        user=argument_list[i+1]



print("Part 0: Input Confirmation")

if origin=='':
    print("Error: No blocks for donut accessibility mapping are defined")
    print("--Calculation Finished--")
    sys.exit()
else:
    print("Mapping for block: {}".format(origin))
if schema=='':
    print("Error: No database schema is defined")
    print("--Calculation Finished--")
    sys.exit()
if input_path=='':
    print("Error: No input path is defined")
    print("--Calculation Finished--")
    sys.exit()
if input_file=='':
    print("Warning: No valid travel time matrix is found. A calculation of travel time matrix will be conducted")

if output_path=='':
    print("Error: No output path is defined")
    print("--Calculation Finished--")
    sys.exit()
if dbname=='' or user=='':
    print("Error: No bdname is defined, unable to connect the database")
    print("--Calculation Finished--")
    sys.exit()




conn = pg.connect("dbname={} user={}".format(dbname, user))
cur = conn.cursor()


print("Part 1: Measure the reachable destinations for selected blocks")

def table_yes(table_name):
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, table_name))
    if cur.fetchone()[0]==False:
        cur.execute("create table {}.{} (origin varchar(15), destination varchar(15), deptime bigint, traveltime bigint)".format(schema, table_name))
        conn.commit()
    else:
        cur.execute("drop table {}.{}".format(schema, table_name))
        cur.execute("create table {}.{} (origin varchar(15), destination varchar(15), deptime bigint, traveltime bigint)".format(schema, table_name))
        conn.commit()



def analyst_config(origin_shapefile, output_path):   
    config_file_parsed["originShapefile"]="{}".format(origin_shapefile)
    config_file_parsed["outputPath"]="{}".format(output_path)
    return config_file_parsed

def write_config(origin_config_file, origin):
    file_name="analyst_config_block_{}.json".format(origin)
    with open(file_name,'w') as outfile:
        json.dump(origin_config_file, outfile)


def csv_file_generate(origin, input_path, blockwac):
    global csv_output_path
    csv_output_path=os.path.join(input_path,"tt_matrix_block{}".format(origin))
    global selected_block
    selected_block=os.path.join(input_path,"block_{}.shp".format(origin))
    
    with fiona.open("{}".format(blockwac)) as input:
        meta=input.meta
        with fiona.open('{}'.format(selected_block),'w',**meta) as output:
            for feature in input:
                if feature['properties']['GEOID10']==origin:
                    output.write(feature)
                    
      
    origin_config_file=analyst_config(selected_block, csv_output_path)
    write_config(origin_config_file, origin)

    config_file_name="analyst_config_block_{}.json".format(origin)
    subprocess.call(['java','-Xmx10G','-jar','AOBatchAnalyst-0.2.3-all.jar', 'matrix','{}'.format(config_file_name)])
    
        
table_name="ttmatrix_donut_mapping"
table="{}.{}".format(schema, table_name)
    
tiger_block="census2010tigerblock"
block_table="{}.{}".format(schema, tiger_block)

table_yes(table_name)

if input_path!='' and input_file!='':    
    csv_file=os.path.join(input_path, input_file)
    print("Loading Data from {}".format(csv_file))
    cur.execute("COPY {} from '{}' DELIMITER ',' CSV HEADER".format(table, csv_file))
    conn.commit()
    
else:
    print("Calculating travel time matrix for Block {}".format(origin))
    blockwac=os.path.join(input_path, "blockswac.shp")                    
    config_file=open("analyst_config.json","r")
    config_file_parsed=json.load(config_file)

    csv_file_generate(origin, input_path, blockwac)    

    cur.execute("COPY {} from '{}' DELIMITER ',' CSV HEADER".format(table, csv_output_path))
    conn.commit()


time_range_upper=[9, 14, 19, 24, 29, 34, 44, 59, 80]
time_range_lower=[0, 10, 15, 20, 25, 30, 35, 45, 60]



def slcted_dist(lower_range, upper_range):
    range_name="slct_dist_range_{}_{}min".format(lower_range, upper_range)
    range_table="{}.{}".format(schema, range_name)
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, range_name))
    if cur.fetchone()[0]==False:
        cur.execute("select geoid10, geom into {} from {} where geoid10 in (select destination from {} where origin='{}' and round(traveltime/60)>={} and round(traveltime/60)<={}) and aland10!=0".format(range_table, block_table, table, origin, lower_range, upper_range))
        conn.commit()
    else:
        cur.execute("drop table {}".format(range_table))
        cur.execute("select geoid10, geom into {} from {} where geoid10 in (select destination from {} where origin='{}' and round(traveltime/60)>={} and round(traveltime/60)<={}) and aland10!=0".format(range_table, block_table, table, origin, lower_range, upper_range))
        conn.commit()


cur.execute("select count(*) from {} where origin='{}'".format(table, origin))
count_origin=cur.fetchone()[0]
if count_origin==0:
    print("Warning: The loaded travel time matrix does not include the results for origin {}".format(origin))
    print("Calculating travel time matrix for Block {}".format(origin))

    blockwac=os.path.join(input_path, "blockswac.shp")                    
    config_file=open("analyst_config.json","r")
    config_file_parsed=json.load(config_file)

    csv_file_generate(origin, input_path, blockwac)    

    cur.execute("COPY {} from '{}' DELIMITER ',' CSV HEADER".format(table, csv_output_path))
    conn.commit()
    

for i, value in enumerate(time_range_upper):
    
    upper_value=value
    lower_value=time_range_lower[i]
    range_name="slct_dist_range_{}_{}min".format(lower_value, upper_value)
    range_table="{}.{}".format(schema, range_name)
    
    print("Selecting for range {} to {}min".format(lower_value, upper_value))
    slcted_dist(lower_value, upper_value)
    
    

print("Part 2: Clean up the selections")

print("2.1 Water resources selection")

def slcted_water():
    water_name="slct_water_sources"
    water_table="{}.{}".format(schema,water_name)
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, water_name))
    if cur.fetchone()[0]==False:
        cur.execute("select geoid10, geom into {} from {} where aland10=0".format(water_table,block_table))
        conn.commit()

slcted_water()


print("2.2 Other unreachable blocks selection")


def slcted_unreachable():
    unreach_name="slct_unreachable_blocks"
    unreach_table="{}.{}".format(schema, unreach_name)
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, unreach_name))
    if cur.fetchone()[0]==False:
        cur.execute("select geoid10, geom into {} from {} where geoid10 in (select destination from {} where origin='{}' and traveltime>4800) and aland10!=0".format(unreach_table, block_table, table, origin))
        conn.commit()
    else:
       cur.execute("drop table {}".format(unreach_table))
       cur.execute("select geoid10, geom into {} from {} where geoid10 in (select destination from {} where origin='{}' and traveltime>4800) and aland10!=0".format(unreach_table, block_table, table, origin))
       conn.commit()

def nearest_blocks(geoid10):
    cur.execute("select avg(traveltime) from {} where destination in (SELECT geoid10 FROM {} blocks ORDER BY blocks.geom<-> (select geom from {} blocks where geoid10='{}')  limit 20) and origin='{}' and traveltime<=4800".format(table, block_table, block_table, geoid10, origin, origin))
    avg_tt=cur.fetchone()[0]
    '''
    tt=0
    num=0
    for i, value in enumerate(near_blocks):
        if value[1]<4800:
            tt+=value[1]
            num+=1
    avg_tt=tt/num
    '''
    return avg_tt

def range_determine(avg_tt):
    global range_insert_name
    for i, value in enumerate(time_range_upper):        
        upper_value=value
        lower_value=time_range_lower[i]        
        if round(avg_tt/60)>=lower_value and round(avg_tt/60)<=upper_value:            
            range_insert_name="slct_dist_range_{}_{}min".format(lower_value, upper_value)
    return range_insert_name
    

slcted_unreachable()

print("2.3 Clean up other unreachable blocks")
unreach_name="slct_unreachable_blocks"
unreach_table="{}.{}".format(schema, unreach_name)

cur.execute("select geoid10 from {} order by geoid10 asc".format(unreach_table))
unreach_blocks=cur.fetchall()

for block in unreach_blocks:
    tt=nearest_blocks(block[0])
    range_table="{}.{}".format(schema, range_determine(tt))
    cur.execute("insert into {} select geoid10, geom from {} where geoid10='{}'".format(range_table, block_table, block[0]))
    conn.commit()
    if block[0]=='271390803022013':  
       print(("insert into {} select geoid10, geom from {} where geoid10='{}'".format(range_table, block_table, block[0])))
    


print("Part 3: Edit Shapefiles and give a final donut shapefiles")

def union_polygon(shpfile_input, shapefile_output,field):
    with collection("{}.shp".format(shpfile_input), "r") as input:
        shp_schema = { 'geometry': 'Polygon', 'properties': { 'name': 'str' } }
        with collection("{}.shp".format(shapefile_output), "w", "ESRI Shapefile", shp_schema) as output:
            shapes = []
            for f in input:
                shapes.append(shape(f['geometry']))
            merged = cascaded_union(shapes)
            output.write({'properties': {'name': '{}'.format(field_name)}, 'geometry': mapping(merged)})


    
def buffer_polygon(shpfile_input, shpfile_output, field):
    with collection("{}.shp".format(shpfile_input), "r") as input:
        shp_schema = { 'geometry': 'Polygon', 'properties': { 'name': 'str' } }
        with collection("{}.shp".format(shpfile_output), "w", "ESRI Shapefile", shp_schema) as output:
            for polygon in input:
                output.write({'properties': {'name': '{}'.format(field)}, 'geometry': mapping(shape(polygon['geometry']).buffer(10))})

print("3.1 Write water sources into shapefile")


water_name="slct_water_sources"
water_table="{}.{}".format(schema,water_name)
code="select geoid10, geom from {}".format(water_table)
subprocess.call(['pgsql2shp','-f','{}water_sources'.format(output_path),'-h', 'localhost','-u','nexusadmin','aodb-fhwa','{}'.format(code)])

shpfile_input="{}/water_sources".format(output_path)
shpfile_output="{}/unioned_water_sources".format(output_path)
field_name='water_source'

union_polygon(shpfile_input,shpfile_output, field_name)

print("3.2 Write donut access into shapefiles")

for i, value in enumerate(time_range_upper):
    
    upper_value=value
    lower_value=time_range_lower[i]
    range_name="slct_dist_range_{}_{}min".format(lower_value, upper_value)
    range_table="{}.{}".format(schema, range_name)
    print("Writing {}".format(range_name))

    cur.execute("select count(*) from {}".format(range_table))
    num=cur.fetchone()[0]

    if num>0:
        code="select geoid10, geom from {}".format(range_table)
        subprocess.call(['pgsql2shp','-f','{}block_{}_{}_{}min'.format(output_path, origin, lower_value, upper_value),'-h', 'localhost','-u','{}'.format(user),'{}'.format(dbname),'{}'.format(code)])
        shpfile_input="{}block_{}_{}_{}min".format(output_path, origin, lower_value, upper_value)
        shpfile_output="{}unioned_block_{}_{}_{}min".format(output_path, origin, lower_value, upper_value)
        field_name='tt_range_{}_{}min'.format(lower_value, upper_value)
        union_polygon(shpfile_input, shpfile_output,field_name)
        

print("3.3 Union all the shapefiles and get the final donut access")

final_shp_output="{}block_{}_donut_access".format(output_path, origin)
shp_final_schema = { 'geometry': 'Polygon', 'properties': { 'name': 'str' } }

with collection("{}.shp".format(final_shp_output),"w", "ESRI Shapefile", shp_final_schema) as output:
    water_shp_input="{}unioned_water_sources".format(output_path)
    field_name="water_source"
    with collection("{}.shp".format(water_shp_input), "r") as input:
        for polygon in input:
            output.write({'properties': {'name': '{}'.format(field_name)}, 'geometry': mapping(shape(polygon['geometry']))})
            
    for i, value in enumerate(time_range_upper):
        
        upper_value=value
        lower_value=time_range_lower[i]
        range_name="slct_dist_range_{}_{}min".format(lower_value, upper_value)
        range_table="{}.{}".format(schema, range_name)
        cur.execute("select count(*) from {}".format(range_table))
        num=cur.fetchone()[0]
        
        if num>0:
            
            final_shp_input="{}unioned_block_{}_{}_{}min".format(output_path, origin, lower_value, upper_value)
            field_name='tt_range_{}_{}min'.format(lower_value, upper_value)
            
            with collection("{}.shp".format(final_shp_input), "r") as input:                
                for polygon in input:                    
                    output.write({'properties': {'name': '{}'.format(field_name)}, 'geometry': mapping(shape(polygon['geometry']))})


print("3.4 Add IDs for each donut and water sources")
        
with fiona.open('{}.shp'.format(final_shp_output),'r') as input:
    shp_schema=input.schema.copy()
    input_crs=input.crs
    shp_schema['properties']['num']='int'
    with fiona.open("{}.shp".format(final_shp_output), "w", "ESRI Shapefile", shp_schema, input_crs) as output:
        for elem in input:            
            if elem['properties']['name']=="water_source":
                elem['properties']['num']=999
            for i, value in enumerate(time_range_upper):
                upper_value=value
                lower_value=time_range_lower[i]
                if elem['properties']['name']=="tt_range_{}_{}min".format(lower_value,upper_value):
                    elem['properties']['num']=i
                    
            output.write({'properties':elem['properties'],'geometry':mapping(shape(elem['geometry']))})













'''
schema="tt_matrix"



origin="270190911002005"


print("Part 1. Select the specific origin to measure the travel time")



with fiona.open("package_tools/blockswac.shp") as input:
    meta=input.meta
    with fiona.open('package_tools/block_{}.shp'.format(origin),'w',**meta) as output:
        for feature in input:
            if feature['properties']['GEOID10']==origin:
                output.write(feature)


print("Part 2: Measure the travel time matrix based on selected origin")

config_file=open("analyst_config.json","r")
config_file_parsed=json.load(config_file)


def analyst_config(origin):
    config_file_parsed["originShapefile"]="package_tools/block_{}.shp".format(origin)
    config_file_parsed["outputPath"]="package_tools/ttmatrix_block_{}.csv".format(origin)
    return config_file_parsed

def write_config(origin_config_file, origin):
    file_name="analyst_config_block_{}.json".format(origin)
    with open(file_name,'w') as outfile:
        json.dump(origin_config_file, outfile)     


origin_config_file=analyst_config(origin)
write_config(origin_config_file, origin)

config_file_name="analyst_config_block_{}.json".format(origin)
subprocess.call(['java','-Xmx10G','-jar','AOBatchAnalyst-0.2.3-all.jar', 'matrix','{}'.format(config_file_name)])

print("Part 3: Copy results in the database and make the selections based on time ranges")

def table_yes(table_name):
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, table_name))
    if cur.fetchone()[0]==False:
        cur.execute("create table {}.{} (origin varchar(15), destination varchar(15), deptime bigint, traveltime bigint)".format(schema, table_name))
        conn.commit()
    else:
        cur.execute("drop table {}.{}".format(schema, table_name))
        cur.execute("create table {}.{} (origin varchar(15), destination varchar(15), deptime bigint, traveltime bigint)".format(schema, table_name))
        conn.commit()

table_name="ttmatrix_block_{}".format(origin)
table="{}.{}".format(schema, table_name)
matrix_path="/Users/Rachel/Desktop/scripts/bin_analysis/package_tools/"
matrix_name="ttmatrix_block_{}.csv".format(origin)

tiger_block="census2010tigerblock"
block_table="{}.{}".format(schema, tiger_block)

matrix_table=os.path.join(matrix_path, matrix_name)

table_yes(table_name)
cur.execute("COPY {} from '{}' DELIMITER ',' CSV HEADER".format(table, matrix_table))
conn.commit()

time_range_upper=[9, 14, 19, 24, 29, 34, 44, 59, 80]
time_range_lower=[0, 10, 15, 20, 25, 30, 35, 45, 60]



def slcted_dist(lower_range, upper_range):
    range_name="slct_dist_range_{}_{}min".format(lower_range, upper_range)
    range_table="{}.{}".format(schema, range_name)
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, range_name))
    if cur.fetchone()[0]==False:
        cur.execute("select geoid10, geom into {} from {} where geoid10 in (select destination from {} where round(traveltime/60)>={} and round(traveltime/60)<={}) and aland10!=0".format(range_table, block_table, table, lower_range, upper_range))
        conn.commit()
    else:
        cur.execute("drop table {}".format(range_table))
        cur.execute("select geoid10, geom into {} from {} where geoid10 in (select destination from {} where round(traveltime/60)>={} and round(traveltime/60)<={}) and aland10!=0".format(range_table, block_table, table, lower_range, upper_range))
        conn.commit()

for i, value in enumerate(time_range_upper):
    
    upper_value=value
    lower_value=time_range_lower[i]
    range_name="slct_dist_range_{}_{}min".format(lower_value, upper_value)
    range_table="{}.{}".format(schema, range_name)
    
    print("Selecting for range {} to {}min".format(lower_value, upper_value))
    slcted_dist(lower_value, upper_value)

print("Part 4: Clean up the blank holes referring to the unreable blocks")

print("4.1 Water resources selection")

def slcted_water():
    water_name="slct_water_sources"
    water_table="{}.{}".format(schema,water_name)
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, water_name))
    if cur.fetchone()[0]==False:
        cur.execute("select geoid10, geom into {} from {} where aland10=0".format(water_table,block_table))
        conn.commit()

slcted_water()


print("4.2 Other unreachable blocks selection")


def slcted_unreachable():
    unreach_name="slct_unreachable_blocks"
    unreach_table="{}.{}".format(schema, unreach_name)
    cur.execute("select exists (select 1 from information_schema.tables where table_schema='{}' and table_name='{}')".format(schema, unreach_name))
    if cur.fetchone()[0]==False:
        cur.execute("select geoid10, geom into {} from {} where geoid10 in (select destination from {} where traveltime>4800) and aland10!=0".format(unreach_table, block_table, table))
        conn.commit()

def nearest_blocks(geoid10):
    cur.execute("select destination, traveltime from {} where destination in (SELECT geoid10 FROM {} blocks ORDER BY blocks.geom<-> (select geom from {} blocks where geoid10='{}') LIMIT 20)".format(table, block_table, block_table, geoid10))
    near_blocks=cur.fetchall()
    tt=0
    num=0
    for i, value in enumerate(near_blocks):
        if value[1]<4800:
            tt+=value[1]
            num+=1
    avg_tt=tt/num
    return avg_tt

def range_determine(avg_tt):
    for i, value in enumerate(time_range_upper):
        upper_value=value
        lower_value=time_range_lower[i]
        if round(avg_tt/60)>=lower_value and round(avg_tt/60)<upper_value:
            global range_insert_name
            range_insert_name="slct_dist_range_{}_{}min".format(lower_value, upper_value)
    return range_insert_name

slcted_unreachable()

print("4.3 Clean up other unreachable blocks")
unreach_name="slct_unreachable_blocks"
unreach_table="{}.{}".format(schema, unreach_name)

cur.execute("select geoid10 from {} order by geoid10 asc".format(unreach_table))
unreach_blocks=cur.fetchall()

for block in unreach_blocks:
    #print(block[0])
    tt=nearest_blocks(block[0])
    range_table_name=range_determine(tt)
    range_table="{}.{}".format(schema, range_table_name)
    cur.execute("insert into {} select geoid10, geom from {} where geoid10='{}'".format(range_table, block_table, block[0]))
    conn.commit()
    
    

print("Part 5: Edit Shapefiles and give a final donut shapefiles")

def union_polygon(shpfile_input, shapefile_output,field):
    with collection("{}.shp".format(shpfile_input), "r") as input:
        shp_schema = { 'geometry': 'Polygon', 'properties': { 'name': 'str' } }
        with collection("{}.shp".format(shapefile_output), "w", "ESRI Shapefile", shp_schema) as output:
            shapes = []
            for f in input:
                shapes.append(shape(f['geometry']))
            merged = cascaded_union(shapes)
            output.write({'properties': {'name': '{}'.format(field_name)}, 'geometry': mapping(merged)})


    
def buffer_polygon(shpfile_input, shpfile_output, field):
    with collection("{}.shp".format(shpfile_input), "r") as input:
        shp_schema = { 'geometry': 'Polygon', 'properties': { 'name': 'str' } }
        with collection("{}.shp".format(shpfile_output), "w", "ESRI Shapefile", shp_schema) as output:
            for polygon in input:
                output.write({'properties': {'name': '{}'.format(field)}, 'geometry': mapping(shape(polygon['geometry']).buffer(10))})

print("5.1 Write water sources into shapefile")


water_name="slct_water_sources"
water_table="{}.{}".format(schema,water_name)
code="select geoid10, geom from {}".format(water_table)
subprocess.call(['pgsql2shp','-f','/Users/Rachel/Desktop/scripts/bin_analysis/package_tools/shp_results/water_sources','-h', 'localhost','-u','nexusadmin','aodb-fhwa','{}'.format(code)])

shpfile_input="package_tools/shp_results/water_sources"
shpfile_output="package_tools/shp_results/unioned_water_sources"
field_name='water_source'

union_polygon(shpfile_input,shpfile_output, field_name)

print("5.2 Write donut access into shapefiles")

for i, value in enumerate(time_range_upper):
    
    upper_value=value
    lower_value=time_range_lower[i]
    range_name="slct_dist_range_{}_{}min".format(lower_value, upper_value)
    range_table="{}.{}".format(schema, range_name)
    print("Writing {}".format(range_name))

    cur.execute("select count(*) from {}".format(range_table))
    num=cur.fetchone()[0]

    if num>0:
        code="select geoid10, geom from {}".format(range_table)
        subprocess.call(['pgsql2shp','-f','/Users/Rachel/Desktop/scripts/bin_analysis/package_tools/shp_results/bufferUnion/block_{}_{}_{}min'.format(origin, lower_value, upper_value),'-h', 'localhost','-u','nexusadmin','aodb-fhwa','{}'.format(code)])
        shpfile_input="package_tools/shp_results/bufferUnion/block_{}_{}_{}min".format(origin, lower_value, upper_value)
        shpfile_output="package_tools/shp_results/bufferUnion/unioned_block_{}_{}_{}min".format(origin, lower_value, upper_value)
        field_name='tt_range_{}_{}min'.format(lower_value, upper_value)
        union_polygon(shpfile_input, shpfile_output,field_name)
        

print("5.3 Union all the shapefiles and get the final donut access")

final_shp_output="package_tools/shp_results/block_{}_donut_access".format(origin)
shp_final_schema = { 'geometry': 'Polygon', 'properties': { 'name': 'str' } }

with collection("{}.shp".format(final_shp_output),"w", "ESRI Shapefile", shp_final_schema) as output:
    water_shp_input="package_tools/shp_results/unioned_water_sources"
    field_name="water_source"
    with collection("{}.shp".format(water_shp_input), "r") as input:
        for polygon in input:
            output.write({'properties': {'name': '{}'.format(field_name)}, 'geometry': mapping(shape(polygon['geometry']))})
            
    for i, value in enumerate(time_range_upper):
        
        upper_value=value
        lower_value=time_range_lower[i]
        range_name="slct_dist_range_{}_{}min".format(lower_value, upper_value)
        range_table="{}.{}".format(schema, range_name)
        cur.execute("select count(*) from {}".format(range_table))
        num=cur.fetchone()[0]
        
        if num>0:
            
            final_shp_input="package_tools/shp_results/bufferUnion/unioned_block_{}_{}_{}min".format(origin, lower_value, upper_value)
            field_name='tt_range_{}_{}min'.format(lower_value, upper_value)
            
            with collection("{}.shp".format(final_shp_input), "r") as input:                
                for polygon in input:                    
                    output.write({'properties': {'name': '{}'.format(field_name)}, 'geometry': mapping(shape(polygon['geometry']))})


print("5.4 Add IDs for each donut and water sources")
        
with fiona.open('{}.shp'.format(final_shp_output),'r') as input:
    shp_schema=input.schema.copy()
    input_crs=input.crs
    shp_schema['properties']['num']='int'
    with fiona.open("{}.shp".format(final_shp_output), "w", "ESRI Shapefile", shp_schema, input_crs) as output:
        for elem in input:            
            if elem['properties']['name']=="water_source":
                elem['properties']['num']=999
            for i, value in enumerate(time_range_upper):
                upper_value=value
                lower_value=time_range_lower[i]
                if elem['properties']['name']=="tt_range_{}_{}min".format(lower_value,upper_value):
                    elem['properties']['num']=i
                    
            output.write({'properties':elem['properties'],'geometry':mapping(shape(elem['geometry']))})


'''    


print("---------------------Calculating Finished------------------------")


