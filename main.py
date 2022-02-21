# Import packges

import pandas as pd
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Point, asPoint
from shapely import geometry
from owslib.wfs import WebFeatureService
from owslib.etree import etree
from owslib.fes import *
from requests import Request
from sqlalchemy import create_engine
import geoalchemy2

# Creating engine to connect the DB
engine = create_engine("postgresql://adm_geout:ssdgeout@10.207.30.15:5432/geout")


# Filters
def remove_sitdurhs(durhs, proc):
    durhs = durhs.loc[(durhs['situacaodurh'] != 'Cancelada')
                      & (durhs['situacaodurh'] != 'Incompleta')
                      & (durhs['situacaodurh'] != 'Retificada')]  # entrada nova
    proc = proc.loc[(proc['situacaodurh'] != 'Cancelada')
                    & (proc['situacaodurh'] != 'Incompleta')
                    & (proc['situacaodurh'] != 'Retificada')]  # entrada nova
    return durhs, proc


def remove_user(durhs, proc):
    durhs = durhs.loc[(durhs['nomeusuario'] != 'USUARIO REQUERENTE EXTERNO')
                      & (durhs['nomeusuario'] != 'MARCOS VINICIUS ALVES DA COSTA')
                      & (durhs['nomeusuario'] != 'CASA CIVIL')
                      & (durhs['nomeusuario'] != 'JOAO VICTOR JULIANO CARVALHO')
                      & (durhs['nomeusuario'] != 'FULANO')
                      & (durhs['nomeusuario'] != 'USUÁRIO EXTERNO')
                      & (durhs['nomeusuario'] != 'EMPRESA TESTE')]
    proc = proc.loc[(proc['nomeusuario'] != 'USUARIO REQUERENTE EXTERNO')
                    & (proc['nomeusuario'] != 'MARCOS VINICIUS ALVES DA COSTA')
                    & (proc['nomeusuario'] != 'CASA CIVIL')
                    & (proc['nomeusuario'] != 'JOAO VICTOR JULIANO CARVALHO')
                    & (proc['nomeusuario'] != 'FULANO')
                    & (proc['nomeusuario'] != 'USUÁRIO EXTERNO')
                    & (proc['nomeusuario'] != 'EMPRESA TESTE')]
    return durhs, proc


def remove_ident(durhs, proc):
    durhs = durhs.loc[(durhs['identificacao'] != '300.029.610-75')
                      & (durhs['identificacao'] != '010.513.361-20')
                      & (durhs['identificacao'] != '21.797.766/0001-19')
                      & (durhs['identificacao'] != '264.637.650-23')
                      & (durhs['identificacao'] != '212.348.940-92')
                      & (durhs['identificacao'] != '133.658.510-20')
                      & (durhs['identificacao'] != '65.403.974/0001-61')]
    proc = proc.loc[(proc['identificacao'] != '300.029.610-75')
                    & (proc['identificacao'] != '010.513.361-20')
                    & (proc['identificacao'] != '21.797.766/0001-19')
                    & (proc['identificacao'] != '264.637.650-23')
                    & (proc['identificacao'] != '212.348.940-92')
                    & (proc['identificacao'] != '133.658.510-20')
                    & (proc['identificacao'] != '65.403.974/0001-61')]
    return durhs, proc


def remove_sitout(durhs, proc):
    durhs = durhs.loc[(durhs['situacaooutorga'] != 'Incompleto')
                      & (durhs['situacaooutorga'] != 'Vencida')
                      & (durhs['situacaooutorga'] != 'Excluído')
                      & (durhs['situacaooutorga'] != 'Cancelado')
                      & (durhs['situacaooutorga'] != 'Retificado')]  # Entrada nova
    proc = proc.loc[(proc['situacaooutorga'] != 'Incompleto')
                    & (proc['situacaooutorga'] != 'Vencida')
                    & (proc['situacaooutorga'] != 'Excluído')
                    & (proc['situacaooutorga'] != 'Cancelado')
                    & (proc['situacaooutorga'] != 'Retificado')]  # Entrada nova
    return durhs, proc


def remove_proc(durhs, proc):
    durhs = durhs.loc[(durhs['tipoprocesso'] != 'AUTORIZAÇÃO PARA PERFURAÇÃO DE POÇO')
                      & (durhs['tipoprocesso'] != 'REENTRADA - AUTORIZAÇÃO PARA PERFURAÇÃO DE POÇO')
                      & (durhs['tipoprocesso'] != 'TRANSFERÊNCIA DE OUTORGA OU MUDANÇA DE TITULARIDADE')
                      & (durhs['tipoprocesso'] != 'DESISTÊNCIA DE OUTORGA')]  # Entrada nova
    proc = proc.loc[(proc['tipoprocesso'] != 'AUTORIZAÇÃO PARA PERFURAÇÃO DE POÇO')
                    & (proc['tipoprocesso'] != 'REENTRADA - AUTORIZAÇÃO PARA PERFURAÇÃO DE POÇO')
                    & (proc['tipoprocesso'] != 'TRANSFERÊNCIA DE OUTORGA OU MUDANÇA DE TITULARIDADE')
                    & (proc['tipoprocesso'] != 'DESISTÊNCIA DE OUTORGA')]  # Entrada nova
    return durhs, proc


# Run function
def run(durhs, proc):
    durhs, proc = remove_sitdurhs(durhs, proc)
    durhs, proc = remove_user(durhs, proc)
    durhs, proc = remove_ident(durhs, proc)
    durhs, proc = remove_sitout(durhs, proc)
    durhs, proc = remove_proc(durhs, proc)
    dif_proc = proc.overlay(durhs, how="difference")
    produto_final = durhs.append(dif_proc)
    produto_final.to_postgis("durhs_filtradas_completas", engine, if_exists="replace")
    return


# URL for WFS backend
url = "https://siga.meioambiente.go.gov.br/gs/ows?service=WFS&"

# Initialize
wfs = WebFeatureService(url=url, username='paulo.smaia', password='Tumtum311', version="1.1.0", timeout=360)

# Specify the parameters for fetching the data
data_proc = wfs.getfeature(typename='geonode:vw_espacializacao_processo_weboutorga', outputFormat='gml3')
data_durh = wfs.getfeature(typename='geonode:vw_espacializacao_durh_weboutorga', outputFormat='gml3')
# Read data as GeoDataFrame
durhs = gpd.read_file(data_durh)
durhs = durhs[durhs.geometry.type == 'Point']
proc = gpd.read_file(data_proc)
proc = proc[proc.geometry.type == 'Point']
# durhs['geometry'] = durhs['geometry'].fillna(asPoint(0.0))
run(durhs, proc)

