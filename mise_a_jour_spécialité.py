import mysql.connector
import pandas as pd
import os


# Set the working directory
working_directory = 'K:\Documentation\DATA\Stagiaire HUYNH Minh Nhat Thy\Scraping\onisep\Lycée'
os.chdir(working_directory)

 #PHOENIX
connection_config_dict = {
        'user': 'proxyuser',
        'password': 'H4n7euAqShERxSW',
        'host': '34.76.231.115',
        'database': 'phoenix',
        'raise_on_warnings': True,
        'use_pure': False,
        'autocommit': True,
        }
conn1 = mysql.connector.connect(**connection_config_dict)
cursor = conn1.cursor(buffered=True)

#onisep
onisep_enseignements_de_specialite_de_premiere_generale = pd.read_excel(r"K:\Documentation\DATA\Stagiaire HUYNH Minh Nhat Thy\Scraping\onisep\ideo-enseignements_de_specialite_de_premiere_generale.xlsx",
                   sheet_name="Sheet1")

#edu_high_school
phoenix_edu_high_school = "SELECT * FROM phoenix.edu_high_school"

#edu_speciality
phoenix_edu_speciality = "SELECT * FROM phoenix.edu_speciality"

#edu_high_school_speciality
edu_high_school_speciality = "SELECT * FROM phoenix.edu_high_school_speciality"

#Création des dataframes
phoenix_edu_high_school = pd.read_sql_query(phoenix_edu_high_school,conn1)
phoenix_edu_speciality = pd.read_sql_query(phoenix_edu_speciality,conn1)
edu_high_school_speciality = pd.read_sql_query(edu_high_school_speciality,conn1)

# Créer des dataframes vides pour garder des données trouvées
result_edu_high_school_speciality = pd.DataFrame(columns=['high_school_id', 'speciality_id'])

def chercher_index_de_phoenix_school_id_par_uai(uai, df):
    indices = df.index[df['uai'] == uai].tolist()   
    return indices[0] if indices else None
# A partir un dataframe et une liste des uais donné, on cherche les id correspondent au uai donné dans le dataframe

def chercher_index_phoenix_edu_speciality(spécialités, df):
    spécialités = spécialités.lower()
    indices = df.index[df['name'].str.lower() == spécialités].tolist()
    return indices[0] if indices else None
# A partir un dataframe et une liste des spécialités donné, on cherche les id correspondent au spécialités données dans le dataframe

def updater_spécialités_des_lycées(result_edu_high_school_speciality, phoenix_edu_high_school, phoenix_edu_speciality, onisep_enseignements_de_specialite_de_premiere_generale):
    
    count = 0
    
    for index in onisep_enseignements_de_specialite_de_premiere_generale.index :
        print(index)
        print(onisep_enseignements_de_specialite_de_premiere_generale.loc[index, 'uai'])
        # partir du premier indice, trouver index du tableau high_school a partir de chaque UAI du tableau onisep
        index_de_phoenix_school_id_trouve = chercher_index_de_phoenix_school_id_par_uai(onisep_enseignements_de_specialite_de_premiere_generale['uai'][index], phoenix_edu_high_school)
        if pd.notna(index_de_phoenix_school_id_trouve) > 0 :
            print("On a trouvé le high_school_id correspondante dans Phoenix par uai")
            # chercher lycéé id a partir d'indece du tableau high_school
            high_school_id = phoenix_edu_high_school.at[index_de_phoenix_school_id_trouve, 'id']
            if pd.notna(onisep_enseignements_de_specialite_de_premiere_generale.loc[index, 'Enseignements de spécialité de classe de 1ère générale']) : 
                spécialités_list = onisep_enseignements_de_specialite_de_premiere_generale.loc[index, 'Enseignements de spécialité de classe de 1ère générale'].split(' / ') # Split the string into a list of spécialités
                spécialités_list = list(map(str.lower, spécialités_list)) # Utilisation de la fonction map pour appliquer lower() à chaque élément de la liste
                print(f"spécialités_list : {spécialités_list}")
                # A partir des id trouvés par uai, on retrouve les spécialités enseignées par ce cdf
                for spécialités in spécialités_list : 
                    # on cherche dans la specialite_id de la base de la langue de phoenix
                    index_phoenix_edu_speciality = chercher_index_phoenix_edu_speciality(spécialités, phoenix_edu_speciality)
                    if pd.notna(index_phoenix_edu_speciality) : 
                        print(phoenix_edu_speciality["name"][index_phoenix_edu_speciality])
                    else : 
                        print("Ici c'est l'erreur de manquer des id_spécialités")
                    result_edu_high_school_speciality.loc[len(result_edu_high_school_speciality)] = {'high_school_id': high_school_id, 'speciality_id': phoenix_edu_speciality["id"][index_phoenix_edu_speciality]}    
                    count+=1
        
    #result_edu_high_school_speciality.index += 1  # Shift index to start from 1
    #result_edu_high_school_speciality.reset_index(inplace=True)
    #result_edu_high_school_speciality.rename(columns={'index': 'id'}, inplace=True)    
    print(f"count : {count}")

if __name__ == "__main__":
    
    updater_spécialités_des_lycées(result_edu_high_school_speciality, phoenix_edu_high_school, phoenix_edu_speciality, onisep_enseignements_de_specialite_de_premiere_generale)

#Import du fichier dans la BDD
truncate_cmd = "TRUNCATE TABLE phoenix.edu_high_school_speciality"
cursor.execute(truncate_cmd)
conn1.commit()
print("Table edu_high_school_speciality truncated successfully.")
insert_cmd = "INSERT IGNORE INTO phoenix.edu_high_school_speciality (high_school_id, speciality_id) VALUES (%s, %s)"
values = [tuple(map(int, row)) for row in result_edu_high_school_speciality.to_numpy()] # Car il y en a plusieurs lignes 
cursor.executemany(insert_cmd, values)
conn1.commit()
cursor.close()
conn1.close()
print(f"{cursor.rowcount} rows were inserted.")