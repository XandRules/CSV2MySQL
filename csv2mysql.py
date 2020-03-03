
# ADICIONAR pip install mysql-connector
# ADICIONAR pip install pandas
import mysql.connector
from mysql.connector import Error
from mysql.connector import errorcode
import os
import pandas as pd

class Csv2Mysql():
    df = None
    rede = None
    connection = None
    #construtor padrão da classe
    def __init__(self, file=None):

        self.df = pd.read_csv(file)       
        self.menu() 

    #Configurações de conexão com o Banco
    def openConnectionOnDB(self):
        self.connection = mysql.connector.connect(user='root', password='hkl3025',host='127.0.0.1', database='testecsv2db')    

    #Teste de Conexão com o Banco Mysql
    def testConnectionMySql(self):

        self.openConnectionOnDB()
        print(self.connection)                              
        self.connection.close()    

    #executa uma query informada pelo usuario
    def readMySqlDB(self,query):

        self.openConnectionOnDB()
        cursor = self.connection.cursor()

        cursor.execute(query)

        result = cursor.fetchall()

        for x in result:
            print(x)    

        self.connection.close() 

    #Le arquivo csv com uma estrutura conhecida e grava no banco
    def readCSVAndWriteInMySQLDataBase(self):

        try:
            self.openConnectionOnDB()

            cursor = self.connection.cursor()

            #Nome do Banco testecsv2db 
            #Nome da Tabele compras

            mySql_insert_query = """INSERT IGNORE INTO testecsv2db.compras (home,how_it_works,contact,bought) 
                                VALUES 
                                (%s, %s, %s, %s) """   

            records_to_insert = []

            cursor = self.connection.cursor()

            for i in range(len(self.df.home)): 
                records_to_insert_aux = ((int(self.df.home[i]),int(self.df.how_it_works[i]),int(self.df.contact[i]),int(self.df.bought[i])))     
                records_to_insert.append(records_to_insert_aux)                     
                
            print(records_to_insert)

            cursor.executemany(mySql_insert_query, records_to_insert)                                                    

            self.connection.commit()
            print(cursor.rowcount, "Record inserted successfully into compras table")

            print(records_to_insert)
            cursor.close()

        except mysql.connector.Error as error:
            print("Failed to insert record into compras table {}".format(error))

        finally:
            if (self.connection.is_connected()):
                self.connection.close()
                print("MySQL connection is closed")    

    #Menu para seleção de conteúdo
    def menu(self):

        while(True):
            os.system("cls") #comando de io para limpar a tela, se estiver no linux usar clear ao inves de cls
            print("###                   MENU                         ###")
            print("### Testar Conexão com MySQL              Digite 1 ###")
            print("### Ler Arquivo CSV e Mostrar             Digite 2 ###")
            print("### Ler Arquivo CSV e Gravar no DB        Digite 3 ###")
            print("### Ler Dados Salvos no DB                Digite 4 ###")
            inputMenu = int(input())

            if inputMenu == 1:
                self.testConnectionMySql()

            if inputMenu == 2:
                print(self.df)

            if inputMenu == 3:               
                self.readCSVAndWriteInMySQLDataBase()

            if inputMenu == 4:
                print("Digite a Query ->")
                query = input() # exemplo select * from compras
                self.readMySqlDB(query)    
                    

            print("Pressione qualquer tecla...")
            input()        
     
#arquivo o qual esta sendo lido e manipulado
# importante é preciso que esteja no mesmo diretorio do arquivo fonte ou passar o caminho relativo.     
rede = Csv2Mysql("tracking.csv")



