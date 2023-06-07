import aiohttp
import asyncio
import json
import os

from print_custom_files import print_custom_files

#URL da API do banco VictoriaMetrics
url = "http://172.17.251.34:8428/api/v1/import"

#HTTP header obrigatório de JSON
headers = {"Content-Type": "application/json"}

#HTTP Basic Auth
auth = aiohttp.BasicAuth('ether', 'etherconsult')

#le o arquivo que tem os flows, gerado pelo collector
files = print_custom_files("./")
with open(files[0], 'rb') as f:
    data = [line.rstrip() for line in f if line.rstrip()]

async def send_packet(packet):
    async with aiohttp.ClientSession(headers=headers, auth=auth) as session:
        #transforma o pacote JSON em um dicionário Python
        packet = json.loads(packet)

        #extrai informações relevantes do pacote
        floatTimestamp = float(str(str(packet)[str(packet).find("{")+2:str(packet).find(":")-1]))
        intTimestamp = int(str(floatTimestamp)[:str(floatTimestamp).find(".")+4].replace(".",""))
        host = packet["%s" % floatTimestamp]["client"][0]
        flows = packet["%s" % floatTimestamp]["flows"]

        for flow in flows:
            #remove as chaves do dicionário que não são necessárias para a métrica
            bytes = flow.pop("IN_BYTES")
            flow.pop("IN_PKTS")
            flow.pop("FIRST_SWITCHED")
            flow.pop("LAST_SWITCHED")
            flow.pop("IPV4_NEXT_HOP")
            flow.pop("BGP_IPV4_NEXT_HOP")
            flow.pop("INPUT_SNMP")
            flow.pop("OUTPUT_SNMP")
            flow.pop("NF_F_REV_FLOW_DELTA_BYTES")
            flow.pop("TCP_FLAGS")
            flow.pop("PROTOCOL")
            flow.pop("SRC_TOS")
            flow.pop("SRC_MASK")
            flow.pop("DST_MASK")
            flow.pop("DIRECTION")
            flow.pop("FORWARDING_STATUS")
            flow.pop("UNKNOWN_FIELD_TYPE")

            labels = {"__name__": "netFlow", "job": "flow"}

            for key, value in flow.items():
                flow[key] = str(value)

            labels.update(flow)
            # Cria a string que representa a métrica e seu valor
            write = ("{'metric':%s,'values':[%d],'timestamps':[%s]}" % (labels, bytes*8, intTimestamp)).replace(" ", "").replace("'", '"')

            async with session.post(url, data=write) as response:
                # Verifica se a solicitação foi bem sucedida
                if response.status != 204:
                    print(f"Erro ao enviar o pacote para o VictoriaMetrics: {response.status} - {response.text()}")
                else:
                    print(f"Pacote enviado com sucesso para o VictoriaMetrics")

async def main():
    # Cria uma lista de tarefas assíncronas para enviar cada pacote
    tasks = [send_packet(packet) for packet in data]

    # Executa as tarefas em paralelo e aguarda todas as respostas
    await asyncio.gather(*tasks)

# Executa a função principal do asyncio para enviar os pacotes em paralelo
asyncio.run(main())

os.system("rm *.flow")
