// Subset de CID-10 com códigos mais frequentes em atestados trabalhistas
module.exports = [
  // Lombalgia / coluna
  ['M54.0', 'Paniculite atingindo regiões cervical e dorsal'],
  ['M54.2', 'Cervicalgia'],
  ['M54.3', 'Ciática'],
  ['M54.4', 'Lumbago com ciática'],
  ['M54.5', 'Dor lombar baixa'],
  ['M54.6', 'Dor na coluna torácica'],
  ['M54.8', 'Outras dorsalgias'],
  ['M54.9', 'Dorsalgia não especificada'],

  // Articulações / lesões por esforço
  ['M65.4', 'Tenossinovite estilóide radial (de Quervain)'],
  ['M65.9', 'Sinovite e tenossinovite não especificada'],
  ['M70.0', 'Sinovite crepitante crônica da mão e do punho'],
  ['M75.0', 'Capsulite adesiva do ombro'],
  ['M75.1', 'Síndrome do manguito rotador'],
  ['M75.5', 'Bursite do ombro'],
  ['M77.0', 'Epicondilite medial'],
  ['M77.1', 'Epicondilite lateral'],
  ['M79.7', 'Fibromialgia'],
  ['G56.0', 'Síndrome do túnel do carpo'],

  // Resfriados / vias aéreas
  ['J00', 'Nasofaringite aguda (resfriado comum)'],
  ['J02.9', 'Faringite aguda não especificada'],
  ['J03.9', 'Amigdalite aguda não especificada'],
  ['J04.0', 'Laringite aguda'],
  ['J06.9', 'Infecção aguda das vias aéreas superiores não especificada'],
  ['J11.1', 'Influenza com outras manifestações respiratórias'],
  ['J12.9', 'Pneumonia viral não especificada'],
  ['J15.9', 'Pneumonia bacteriana não especificada'],
  ['J18.9', 'Pneumonia não especificada'],
  ['J20.9', 'Bronquite aguda não especificada'],
  ['J32.9', 'Sinusite crônica não especificada'],
  ['J45.9', 'Asma não especificada'],

  // Sistema digestivo
  ['A09', 'Diarreia e gastrenterite de origem infecciosa presumida'],
  ['K29.0', 'Gastrite aguda hemorrágica'],
  ['K29.7', 'Gastrite não especificada'],
  ['K30', 'Dispepsia funcional'],
  ['K52.9', 'Gastrenterite e colite não infecciosas não especificadas'],
  ['K59.0', 'Constipação'],
  ['R10.4', 'Outras dores abdominais e as não especificadas'],
  ['R11', 'Náusea e vômitos'],

  // Cabeça / neuro
  ['G43.9', 'Enxaqueca não especificada'],
  ['G44.2', 'Cefaleia tensional'],
  ['R51', 'Cefaleia'],
  ['R42', 'Tontura e instabilidade'],

  // Saúde mental
  ['F32.0', 'Episódio depressivo leve'],
  ['F32.1', 'Episódio depressivo moderado'],
  ['F32.2', 'Episódio depressivo grave sem sintomas psicóticos'],
  ['F32.9', 'Episódio depressivo não especificado'],
  ['F33.0', 'Transtorno depressivo recorrente, episódio atual leve'],
  ['F33.1', 'Transtorno depressivo recorrente, episódio atual moderado'],
  ['F33.2', 'Transtorno depressivo recorrente, episódio atual grave'],
  ['F40.0', 'Agorafobia'],
  ['F41.0', 'Transtorno de pânico'],
  ['F41.1', 'Transtorno de ansiedade generalizada'],
  ['F41.2', 'Transtorno misto ansioso e depressivo'],
  ['F41.9', 'Transtorno de ansiedade não especificado'],
  ['F43.0', 'Reação aguda ao estresse'],
  ['F43.1', 'Estado de estresse pós-traumático'],
  ['F43.2', 'Transtornos de adaptação'],
  ['F48.0', 'Neurastenia (síndrome de fadiga)'],
  ['F51.0', 'Insônia não orgânica'],

  // Cardiovascular / metabolismo
  ['I10', 'Hipertensão essencial (primária)'],
  ['I20.9', 'Angina pectoris não especificada'],
  ['E10', 'Diabetes mellitus tipo 1'],
  ['E11', 'Diabetes mellitus tipo 2'],
  ['E66.9', 'Obesidade não especificada'],
  ['E78.5', 'Hiperlipidemia não especificada'],
  ['E03.9', 'Hipotireoidismo não especificado'],

  // Geniturinário
  ['N30.0', 'Cistite aguda'],
  ['N39.0', 'Infecção do trato urinário de localização não especificada'],
  ['N76.0', 'Vaginite aguda'],
  ['N94.6', 'Dismenorreia não especificada'],

  // Pele
  ['L23.9', 'Dermatite alérgica de contato, causa não especificada'],
  ['L25.9', 'Dermatite de contato não especificada'],
  ['L29.9', 'Prurido não especificado'],
  ['L30.9', 'Dermatite não especificada'],
  ['L70.0', 'Acne vulgar'],

  // Olhos / ouvidos
  ['H10.9', 'Conjuntivite não especificada'],
  ['H66.9', 'Otite média não especificada'],

  // Acidentes / lesões
  ['S00.9', 'Traumatismo superficial da cabeça parte não especificada'],
  ['S01.9', 'Ferimento da cabeça parte não especificada'],
  ['S06.0', 'Concussão'],
  ['S20.2', 'Contusão do tórax'],
  ['S30.0', 'Contusão da região lombar e da pelve'],
  ['S60.9', 'Traumatismo superficial do punho e da mão não especificado'],
  ['S62.6', 'Fratura de outro dedo da mão'],
  ['S63.0', 'Luxação do punho'],
  ['S70.0', 'Contusão do quadril'],
  ['S80.0', 'Contusão do joelho'],
  ['S82.6', 'Fratura do maléolo lateral'],
  ['S83.6', 'Entorse e distensão de outras partes e das não especificadas do joelho'],
  ['S90.0', 'Contusão do tornozelo'],
  ['S93.4', 'Entorse e distensão do tornozelo'],
  ['T14.0', 'Traumatismo superficial de região não especificada do corpo'],
  ['T14.1', 'Ferimento de região não especificada do corpo'],
  ['T14.9', 'Traumatismo não especificado'],

  // Gravidez / pós-parto
  ['O14.9', 'Pré-eclampsia não especificada'],
  ['O20.0', 'Ameaça de aborto'],
  ['O20.9', 'Hemorragia do início da gravidez não especificada'],
  ['O21.0', 'Hiperêmese leve da gravidez'],
  ['O21.1', 'Hiperêmese gravídica com distúrbios metabólicos'],
  ['O60', 'Trabalho de parto pré-termo'],
  ['Z32.1', 'Gravidez confirmada'],
  ['Z34.9', 'Supervisão de gravidez normal não especificada'],
  ['Z39.0', 'Cuidados e exame logo após o parto'],
  ['Z39.2', 'Acompanhamento pós-natal de rotina'],

  // Genéricos / atestado
  ['Z76.0', 'Pessoa em contato com serviços de saúde para emissão de receita repetida'],
  ['Z02.7', 'Emissão de atestado médico'],
  ['R53', 'Mal-estar e fadiga'],

  // Covid / virais
  ['B34.9', 'Doença viral não especificada'],
  ['U07.1', 'COVID-19, vírus identificado'],
  ['U07.2', 'COVID-19, vírus não identificado'],

  // Cirurgias / pós-operatório
  ['Z48.0', 'Cuidados aos curativos cirúrgicos'],
  ['Z48.8', 'Outros cuidados pós-cirúrgicos especificados'],
  ['Z48.9', 'Cuidados pós-cirúrgicos não especificados'],

  // Odontologia
  ['K00.6', 'Distúrbios da erupção dos dentes'],
  ['K04.0', 'Pulpite'],
  ['K04.7', 'Abscesso periapical sem fístula'],
  ['K05.6', 'Doença periodontal não especificada'],

  // Outros
  ['R50.9', 'Febre não especificada'],
  ['R52.9', 'Dor não especificada'],
  ['R56.0', 'Convulsões febris'],
];
