import sqlite3
import jmespath
import pandas as pd
import numpy as np
import pickle
import json
import re


class Db_Manager():

    def __init__(self, db_type):

        with open(r"RFR_model.pickle", "rb") as input_file:
            self.model = pickle.load(input_file)

        with open("config.json", 'r', encoding='ISO 8859-9') as wr:
            self.js = json.load(wr)

        self.conn = sqlite3.connect('sahibinden_db_v2.db')
        self.cursor = self.conn.cursor()
        self.db_type = db_type

        print(db_type)

    def create_table(self):
        target_urls_fixed_columns = self.js['table_columns']['target_urls_table'].keys()
        car_table_fixed_columns = self.js['table_columns']['car_table'].keys()

        target_urls_fixed_columns = self.clean_column_names(self.js['table_columns']['target_urls_table'].keys())
        dict_values = [k for k in self.js['table_columns']['target_urls_table'].values()]
        fixed_dict = dict((a, b) for a,b in zip(target_urls_fixed_columns, dict_values))
        target_urls_columns = "(" + ",\n".join(
            [f"[{k}] {v}" for k, v in fixed_dict.items()]) + ")"

        car_table_fixed_columns = self.clean_column_names(self.js['table_columns']['car_table'].keys())
        dict_values = [k for k in self.js['table_columns']['car_table'].values()]
        fixed_dict = dict((a, b) for a,b in zip(car_table_fixed_columns, dict_values))
        car_table_columns = "(" + ",\n".join(
            [f"[{k}] {v}" for k, v in fixed_dict.items()]) + ")"

        if self.db_type.lower() == "spider":

            try:
                self.cursor.execute(
                    "CREATE TABLE target_urls\n" + target_urls_columns)
                self.conn.commit()
                print('target_urls table created..')
            except Exception as exc:
                print('Spider table: ', exc)

        elif self.db_type.lower() == "crawler":

            try:
                self.cursor.execute(
                    "CREATE TABLE car_table\n" + car_table_columns)
                self.conn.commit()
                print('car_table table created..')
            except Exception as exc:
                print('Car table: ', exc)


    def get_config_json(self):
        return self.js

    def write(self, table_name, values, many=True):

        if many:
            target_urls_columns = jmespath.search('table_columns.target_urls_table.keys(@)', self.js)
            target_urls_values_placeholder = ', '.join([f":{col}" for col in self.clean_column_names(target_urls_columns)])
            target_urls_columns_placeholder = ', '.join([f"{col}" for col in self.clean_column_names(target_urls_columns)])
            query = f"INSERT OR IGNORE INTO {table_name} ({target_urls_columns_placeholder}) VALUES ({target_urls_values_placeholder})" 
            self.cursor.executemany(query, values)

        else:
            fixed_keys = self.clean_column_names(values.keys())
            dict_values = [k for k in values.values()]
            values = dict((a, b) for a,b in zip(fixed_keys, dict_values))
            car_table_columns = jmespath.search('table_columns.car_table.keys(@)', self.js)
            car_table_values_placeholder = ', '.join([f":{col}" for col in self.clean_column_names(car_table_columns)])
            car_table_columns_placeholder = ', '.join([f"{col}" for col in self.clean_column_names(car_table_columns)])
            query = f"INSERT OR IGNORE INTO {table_name} ({car_table_columns_placeholder}) VALUES ({car_table_values_placeholder})" 
            self.cursor.execute(query, values)

        self.conn.commit()
    
    def clean_column_names(self, name_list):
        trans_dict = str.maketrans('/.)(-+', '______')
        pat = re.compile('^(?=\d)')
        #fixed_dict_keys = [k.replace('3 ', 'uc_').replace('6 ', 'alti_').replace('7 ', 'yedi_').lower().replace('/', '').replace(')', '').replace('.', ' ').replace('  ', '').replace('(', '_').replace('-', '_').replace(' ', '_').replace(' ', '').replace('+', '').replace('__', '_').replace('__', '_') for k in name_list]
        fixed_dict_keys = [pat.sub('num', k.translate(trans_dict).replace('_', '').replace(' ', '')) for k in name_list]
        return fixed_dict_keys

    def check_id(self, id_list):
        column_number = len(id_list)
        value_marks = '(' + column_number * '?,' + ')'
        value_marks = value_marks.replace(',)', ')')
        query = "SELECT * FROM target_urls WHERE id IN " + value_marks
        result = self.cursor.execute(query, id_list).fetchall()
        return result

    def get_column_names(self):
        self.cursor.execute("SELECT * FROM car_table")
        columns = [description[0] for description in self.cursor.description]
        return columns

    def update_row_checked(self, ad_id):
        self.cursor.execute(
            fr"UPDATE target_urls set checked = 1 WHERE id = {ad_id}")
        self.conn.commit()

    def target_url_number(self, table_name):
        result = self.cursor.execute(
            f"SELECT COUNT(*) FROM {table_name} WHERE checked IS NULL")
        return result.fetchall()[0][0]

    def pick_an_ad(self, table_name):
        result = self.cursor.execute(
            f"SELECT * FROM {table_name} WHERE checked IS NULL").fetchone()
        return result

    def update_labels(self, url_list):
        column_number = len(url_list)
        value_marks = '(' + column_number * '?,' + ')'
        value_marks = value_marks.replace(',)', ')')
        query = "UPDATE car_table set label = '1' WHERE url IN " + value_marks
        self.cursor.execute(query, url_list)
        self.conn.commit()

    def predict_rows(self):
        models_of_interest = ['skoda', 'bmw', 'opel', 'fiat', 'audi', 'volkswagen',
                              'mercedes-benz', 'hyundai', 'toyota', 'chevrolet', 'renault',
                              'ford', 'porsche', 'citroen', 'volvo', 'kia', 'Other', 'peugeot',
                              'honda', 'mini', 'mazda', 'tofas', 'nissan', 'seat', 'dacia']
        df = pd.read_sql('SELECT * FROM car_table WHERE checked IS NULL',
                         self.conn).replace('None', np.nan)
        dn = pd.read_csv('paket_seri.csv')
        df.dropna(subset=['fiyat'], inplace=True)
        df['fiyat'] = df['fiyat'].astype(float)
        df['yil'] = df['yil'].astype(int)
        df['km'] = df['km'].astype(int)
        df.rename(columns={
            'cat1': 'urun_tipi',
            'cat2': 'vasita_tipi',
            'cat3': 'marka',
            'cat4': 'seri',
            'cat5': 'model',
            'cat6': 'paket',
            'loc1': 'ulke',
            'loc2': 'sehir',
            'loc3': 'ilce',
            'loc4': 'semt'
        }, inplace=True)

        df = df.dropna(subset=['fiyat', 'motor_gucu', 'km', 'yil'])
        df['motor_gucu'] = df['motor_gucu'].astype(str).str.split(
            '_').map(lambda x: np.median([int(k) for k in x]))
        #df['motor_hacmi'] = df['motor_hacmi'].astype(str).str.split('_').map(lambda x: np.median([int(k) for k in x]) if isinstance(x, list) else x)
        df['paket'] = df['paket'].map(lambda x: x if pd.notnull(x) else 1)
        df['marka'] = df.marka.map(
            lambda x: 'Other' if x not in models_of_interest else x)
        df['seri'] = df['seri'].astype('str')
        df['model'] = df['model'].astype('str')
        df['paket'] = df['paket'].astype('str')
        df['marka'] = df['marka'].astype('str')
        dn['seri'] = dn['seri'].astype('str')
        dn['model'] = dn['model'].astype('str')
        dn['paket'] = dn['paket'].astype('str')
        dn['marka'] = dn['marka'].astype('str')
        df = df.merge(dn[['marka', 'seri', 'model', 'paket', 'paket_label', 'seri_label']], on=[
                      'marka', 'seri', 'model', 'paket'], how='left')
        df['paket_label'] = df['paket_label'].map(
            lambda x: 1 if pd.isna(x) else x)
        df['seri_label'] = df['seri_label'].map(
            lambda x: 1 if pd.isna(x) else x)
        dummy_data = pd.get_dummies(
            df, columns=['marka', 'yakit', 'vites', 'kasa_tipi'], drop_first=True)
        dummy_data.drop(columns=['ad_id', 'label', 'date_string', 'urun_tipi', 'vasita_tipi', 'seri', 'motor_hacmi', 'model', 'paket', 'cat0', 'ulke', 'sehir', 'ilce', 'semt', 'loc5', 'cekis', 'renk', 'garanti', 'plaka_uyruk', 'kimden', 'goruntulu_arama_ile_gorulebilir', 'ilan_aks', 'url',
                                 'checked', "front-bumper", "front-hood", "roof", "front-right-mudguard", "front-right-door", "rear-right-door", "rear-right-mudguard", "front-left-mudguard", "front-left-door", "rear-left-door", "rear-left-mudguard", "rear-hood", "rear-bumper", "description", "title"], inplace=True)
        dummy_data.dropna(subset=['fiyat'])
        X = dummy_data.drop(columns='fiyat')
        print(list(X.columns))
        if df.shape[0] == 0:
            return []

        df['predictions'] = np.expm1(self.model.predict(X))
        df['predictions'] = df['predictions'].map(lambda x: round(x))
        df['deviation'] = df.apply(lambda x: (
            x['fiyat'] - x['predictions']) / x['fiyat'] * 100, axis=1)
        ads_list = df['ad_id'].to_list()
        column_number = len(ads_list)
        value_marks = '(' + column_number * '?,' + ')'
        value_marks = value_marks.replace(',)', ')')
        query = "UPDATE car_table set checked = 1 WHERE ad_id IN " + value_marks
        self.cursor.execute(query, ads_list)
        self.conn.commit()
        return df

    def close_session(self):
        self.conn.close()
