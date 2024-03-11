def etl_pipeline(csv_file, db_file, table_name):
    import pandas as pd
    from sqlalchemy import create_engine

    #Extract
    data = pd.read_csv(csv_file)

    #Transform
    transformed_data = data.fillna("")
    '''owner,complete,UUID,alphaTitle,IMDBLink,
       displayTitle,year,rating,runTime,director,
       starring,genres,tags,formats,synopsis,imageLink,Amandas'''
    
    #actors = transformed_data['starring']
    

    '''print(actors)'''

    #Load
    engine = create_engine('sqlite:///'+ db_file, echo=False)
    transformed_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
 
    #Confirm
    ##print(transformed_data)
    db_df = pd.read_sql_query(f"SELECT * FROM {table_name}", con=engine)
    '''print(db_df)'''

def main():
    fileName = 'movieCollection/taraMovies.csv'
    dbName = 'movieCollection/movies.db'
    tableName = 'movies'
    etl_pipeline(fileName, dbName, tableName)


main()