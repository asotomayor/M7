import csv

from dumbo import main


def load_comunidades_provincias(provincias_files):
    comunidades = {}
    try:
        # Read table - Comunidad_Autonoma;Provincia
        with open(provincias_files) as f:
            reader = csv.reader(f, delimiter=';', quotechar='"', doublequote=False)
            reader.next()
            for line in reader:
                comunidades[line[1]] = line[0]

    except:
        pass

    return comunidades


class Parse_contratos_municipios_mapper:
    def __init__(self):
        self.contratos = load_comunidades_provincias('./Comunidades_y_provincias.csv')

    def __call__(self, key, value):
        try:
            num_contratos_hombres_prov = 0
            num_contratos_mujeres_prov = 0
            Comunidad_Autonoma = ""

            # Parse file - Contratos_por_municipio.csv
            codigo_mes, provincia, municipio, total_contratos, contratos_hombres, contratos_mujeres = value.split(';')
            int(contratos_hombres)
            int(contratos_mujeres)


            if contratos_hombres > 0:
                num_contratos_hombres_prov += int(contratos_hombres)

            if contratos_mujeres > 0:
                num_contratos_mujeres_prov += int(contratos_mujeres)

            if provincia != "":
                if "vila" in provincia:
                    Comunidad_Autonoma += "Castilla y Leon"
                else:
                    Comunidad_Autonoma += self.contratos[provincia]


            yield Comunidad_Autonoma, (num_contratos_mujeres_prov, num_contratos_hombres_prov)

        except:
            pass


def Join_contratos_municipios_reduce(key, values):
    num_contratos_hombres = 0
    num_contratos_mujeres = 0

    Comunidad_Autonoma = key[:]

    for v in values:
        num_contratos_mujeres_prov, num_contratos_hombres_prov = v[:]

        num_contratos_hombres += int(num_contratos_hombres_prov)
        num_contratos_mujeres += int(num_contratos_mujeres_prov)

    if num_contratos_mujeres > num_contratos_hombres:
       yield Comunidad_Autonoma, (num_contratos_mujeres, num_contratos_hombres)


def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(Parse_contratos_municipios_mapper, Join_contratos_municipios_reduce, opts=inout_opts)


if __name__ == "__main__":
    main(runner)