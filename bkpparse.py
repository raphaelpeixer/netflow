import requests
import json
from print_custom_files import print_custom_files

# URL da API do banco VictoriaMetrics
url = "http://172.17.251.34:8428/api/v1/import"

# HTTP header obrigatório de JSON
headers = {"Content-Type": "application/json"}

#HTTP Basic Auth
auth = ("ether", "etherconsult")

#le o arquivo que tem os flows, gerado pelo collector
data = print_custom_files("./")
with open(data[0], 'rb') as f:
    data = [line.rstrip() for line in f if line.rstrip()]

for packet in data :
    #como o método de leitura do python é somente via string, aqui transforma cada packet em dict para ser compatível com o código
    packet = json.loads(packet)

    #recebe somente o timestamp, com os milésimos
    floatTimestamp = float(str(str(packet)[str(packet).find("{")+2:str(packet).find(":")-1]))

    #guarda o tempo em inteiro, com 3 numeros após o ponto.
    intTimestamp = int(str(floatTimestamp)[:str(floatTimestamp).find(".")+4].replace(".",""))

    #recebe o ip do host
    host = packet["%s" % floatTimestamp]["client"][0]

    #recebe as vars de flow
    flows = packet["%s" % floatTimestamp]["flows"]

    #aqui se os flows estiver zerado, ele skipa para o próximo
    counter = 0
    while(counter < len(flows)):
        #remove o IN_BYTES do dict flow porém armazena na variável bytes
        bytes = flows[counter].pop("IN_BYTES")

        #remove algumas variáveis do dict, porém não armazena em nenhum lugar consequentemente tirando da memória
        flows[counter].pop("IN_PKTS")
        flows[counter].pop("FIRST_SWITCHED")
        flows[counter].pop("LAST_SWITCHED")
        flows[counter].pop("IPV4_NEXT_HOP")
        flows[counter].pop("BGP_IPV4_NEXT_HOP")
        flows[counter].pop("INPUT_SNMP")
        flows[counter].pop("OUTPUT_SNMP")
        flows[counter].pop("NF_F_REV_FLOW_DELTA_BYTES")
        flows[counter].pop("TCP_FLAGS")
        flows[counter].pop("PROTOCOL")
        flows[counter].pop("SRC_TOS")
        flows[counter].pop("SRC_MASK")
        flows[counter].pop("DST_MASK")
        flows[counter].pop("DIRECTION")
        flows[counter].pop("FORWARDING_STATUS")
        flows[counter].pop("UNKNOWN_FIELD_TYPE")

        #define variável labels que é obrigatória no writer para o db
        labels = {"__name__":"netFlow","job":"flow"}

        #transforma todos os elementos em string, tendo em vista que o db não aceita valores diferentes do que strings
        for each in flows[counter]:
            flows[counter][each] = str(flows[counter][each])

        #updateia o dict labels com os valores do flow para que possa ficar tudo em um dict só
        labels.update(flows[counter])
        write = ("{'metric':%s,'values':[%d],'timestamps':[%s]}" % (labels, bytes*8, intTimestamp)).replace(" ", "").replace("'", '"')

        #faz a request HTTP Post para o banco
        response = requests.post(url, data=write, headers=headers, auth=auth)

        counter+=1
