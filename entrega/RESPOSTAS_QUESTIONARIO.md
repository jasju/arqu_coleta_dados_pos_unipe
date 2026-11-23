# Questionário final: Medalhão, Storage, Qualidade e Governança

Respostas baseadas nas saídas dos notebooks Spark (`notebooks/01_bronze`, `02_silver`, `03_gold`) e do complemento
`notebooks/04_governanca_spark.ipynb`.

---

## 1. Object storage

**1. As “pastas” `mercadonordeste/lote1` existem de verdade?**
Não. O MinIO guarda só **objetos com chaves**, e a chave inteira é `mercadonordeste/lote1/pedidos.jsonl`. O console mostra “pastas” porque pede a listagem agrupada pelo delimitador `/`. No notebook, `list_objects_v2(Prefix="mercadonordeste/", Delimiter="/")` devolveu `Prefixos: ['mercadonordeste/lote1/']`: é um prefixo calculado na hora, não um diretório. No experimento de renomear, criamos `teste/a.txt`, fizemos `copy_object` para `teste/b.txt` e `delete_object` em `a.txt`, e a listagem ficou `['teste/b.txt']`. Renomear não existe: são duas operações (copiar e apagar). “Renomear uma pasta” com milhões de objetos significa copiar todos eles.

**2. Por que um object storage não consegue “editar uma linha”?**
O objeto é **imutável e inteiro**: a API só oferece PUT (grava o objeto todo), GET e DELETE, sem escrever no meio de um arquivo. No lugar de editar, ele **grava um objeto novo**. No Iceberg isso fica visível: ao reexecutar o lote da Bronze, os objetos de `lakehouse/bronze/pedidos` foram de **5 para 12** (data 1→2, metadata 4→10), mas só **1 arquivo** faz parte da versão atual. O `DELETE` dos cancelados também não editou o Parquet: criou um snapshot novo que aponta para arquivos novos.

**3. MinIO parado, pandas instalado e o notebook não funciona: o que isso diz?**
Que **armazenamento e processamento são separados**. O dado vive no storage, e o código (pandas ou Spark) só passa por ele: sem o MinIO, não há onde ler nem gravar, mesmo com o motor instalado. O contrário também vale. Os mesmos dados de `s3://warehouse/lakehouse/` foram lidos por três sessões Spark diferentes (notebooks 01, 02 e 03) e pelo Postgres via cópia. Vantagem prática: dá para **escalar ou trocar cada lado de forma independente**. Posso desligar o cluster de processamento à noite e pagar só o storage (barato), ou trocar o pandas pelo Spark sem migrar dado.

**4. Por que landing e warehouse em buckets diferentes?**
Porque têm **donos, regras e permissões diferentes**. A landing é a “caixa de entrada” imutável do que a origem enviou, e o warehouse é a área gerida pelo pipeline. Buckets separados permitem políticas de acesso, versionamento, retenção e ciclo de vida diferentes (a landing pode expirar em 30 dias, por exemplo), além de isolar falhas: um job com bug na Silver não consegue sobrescrever o arquivo original. **Gravar na landing:** só o sistema de origem (ou o processo de carga), sem permissão para ler o warehouse. **Só ler a Bronze:** o job que constrói a Silver e quem faz auditoria/linhagem. Escrever na Bronze é exclusivo do job de ingestão. No laboratório tudo usa `admin`, o que é aceitável só em sala.

## 2. Bronze

**5. Por que a Bronze não remove duplicados nem corrige tipos?**
Porque ela é a **prova do que a origem enviou** e o ponto de reprocessamento. Se a regra de limpeza estiver errada, refazemos a Silver a partir da Bronze sem pedir os arquivos de novo. Removendo duplicados ali, perderíamos: (a) a evidência do problema na origem. A Bronze tem **30.150 linhas para 30.000 pedidos**, ou seja, 150 duplicados que precisam ser reportados ao dono do sistema. (b) A possibilidade de mudar o critério de deduplicação (mais recente, primeira versão…). (c) A linhagem de cada versão do registro. Tipar também pode destruir informação: uma data `dd/MM/yyyy` convertida com o formato errado vira `NULL` e o valor original some.

**6. Para que servem `_ingest_ts`, `_source_file` e `_batch_id`?**
Rastreabilidade e linhagem: **quando** entrou, **de qual arquivo** veio e **de qual lote**. `_batch_id` também é a partição da tabela e a chave da idempotência (`DELETE WHERE _batch_id='lote1'`). `_ingest_ts` é o critério de “versão mais recente” na deduplicação da Silver. Exemplo do notebook: o **pedido 123912** (cliente 2769, status `devolvido`, itens `[{211.62, 6, 2}]`) veio do arquivo `s3a://landing/mercadonordeste/lote1/pedidos.jsonl`, lote **`lote1`**.

**7. Reexecutamos o mesmo lote e as linhas não dobraram. Como? E os objetos?**
A ingestão (`to_bronze`) é idempotente por lote: se a tabela existe, ela faz `DELETE FROM … WHERE _batch_id = 'lote1'` e depois o append. Resultado: `Linhas: 30150 -> 30150 | idempotente: True`. **Na versão Iceberg**, os objetos aumentaram de **5 para 12** (1→2 Parquet, 4→10 metadados). O Iceberg não apaga nada fisicamente: cria um snapshot novo que deixa de apontar para o arquivo antigo, e o arquivo antigo fica guardando o passado (só **1** arquivo está na versão atual). **Na versão pandas**, o número de objetos **não muda**: o código apaga o prefixo `bronze/<tabela>/lote1/` e grava `part-0000.parquet` de novo (só a data de gravação muda). A diferença é que o Iceberg faz idempotência **com histórico**, e por isso permite voltar no tempo e precisa de limpeza (`expire_snapshots`). O pandas faz idempotência **por sobrescrita**: é simples, mas o passado se perde.

**8. O que guardam `data/` e `metadata/`? Por que o pandas não tem `metadata/`?**
`data/` guarda os **Parquet com as linhas** (ex.: `data/_batch_id=lote1/00000-14-…parquet`, 471.776 bytes). `metadata/` guarda o que define a tabela em cada versão. `v1.metadata.json` tem esquema, partições e lista de snapshots. `snap-….avro` é a manifest list de um snapshot. `…-m0.avro` é o manifest, com os arquivos de dados e suas estatísticas. `version-hint.text` aponta a versão atual (no Hadoop catalog). Na versão pandas não existe `metadata/` porque não há formato de tabela: são só Parquet soltos num prefixo, e “a tabela” é “todos os arquivos que estiverem ali”. Riscos: **sem atomicidade** (um leitor no meio de uma gravação vê arquivo faltando ou parcial), **sem histórico/time travel**, **sem controle de esquema** (um arquivo com coluna diferente quebra a leitura) e **sem concorrência segura** (duas gravações simultâneas se sobrescrevem).

## 3. Silver

**9. Três regras da Silver e o que acontece com quem as viola.**
1. **Pedido com cliente inexistente** (join `left_anti` com `silver.clientes`): o pedido vai para a **quarentena** (`pedidos_cliente_orfao`, 40 registros, todos com `cliente_id = 999999`) e seus itens não entram em `itens_pedido`.
2. **Produto com `preco` ou `custo` ≤ 0**: o produto vai para a **quarentena** (`produtos`, 3 registros, ex.: `{"produto_id":55,"preco":-418.73,"custo":328.22}`).
3. **Avaliação com `nota` fora de 1–5**: vai para a **quarentena** (`avaliacoes_nota_invalida`, 25 registros: 8 com nota 0, 7 com nota 6 e 10 com nota 10).

Regras que **não** mandam para a quarentena: e-mail fora do padrão vira `NULL` (o registro fica; 270 casos), e duplicados por `pedido_id`/`cliente_id`/`avaliacao_id` são resolvidos mantendo o `_ingest_ts` mais recente (150 pedidos descartados).

**10. O que é a quarentena e por que é melhor que apagar?**
É uma tabela (`lake.silver.quarentena`) com o registro rejeitado (`registro` em JSON), o **motivo** (`origem`) e **quando** (`_quarentena_ts`). É melhor que apagar porque: (a) **nada some sem rastro**, e a reconciliação fecha (`30000 = 29960 + 40 -> True`); (b) dá para **corrigir e reprocessar**; (c) vira **sinal de qualidade** para o dono da origem; (d) permite auditar se a regra está certa. Exemplo real: `pedidos_cliente_orfao | {"pedido_id":103054,"cliente_id":999999}`. Apagado, esse pedido simplesmente sumiria do faturamento e ninguém saberia que o sistema de origem está gerando pedidos com um cliente “coringa”.

**11. Por que `not_null(email)` é AVISO e não ERRO?**
Porque o e-mail inválido **não invalida o cliente**: ele continua existindo, com pedidos e receita, e só perdeu o canal de contato. Bloquear o pipeline por isso tiraria 270 clientes (5,4%) de todas as análises de vendas. O AVISO deixa o problema **visível e medido** (`total 5000, falhas 270, pct 5.4, AVISO`) sem impedir o uso. Seria **ERRO** quando o e-mail é essencial para o produto de dados, como numa tabela de disparo de campanhas de e-mail marketing, na comunicação obrigatória (nota fiscal, LGPD) ou numa chave de login. Também seria ERRO se o percentual saltasse (ex.: de 5% para 60%), porque isso indica quebra na origem e não sujeira normal.

**12. Por que deduplicar antes de explodir `itens`?**
Porque, explodindo antes, **cada item de um pedido duplicado vira duas linhas**, e o faturamento fica inflado. Na Bronze há 30.150 linhas para 30.000 pedidos distintos. Explodindo primeiro, os itens dos 150 pedidos repetidos entrariam duas vezes e a receita somaria valores fantasmas. Ainda daria para deduplicar depois, mas a chave passaria a ser composta (`pedido_id`, `item_seq`) e mais frágil. Deduplicando antes, ficam 30.000 pedidos e 50.105 itens, e a chave composta `(pedido_id, item_seq)` é única (`chaves duplicadas: 0 -> OK`).

**13. “entrada = silver + quarentena” fechou?**
Sim: `pedidos : 30000 = 29960 (silver) + 40 (quarentena) -> True`. Também fecha para produtos (200 = 197 + 3) e avaliações (6.489 = 6.464 + 25). Se não fechasse, o pipeline estaria **perdendo (ou criando) dados sem avisar**. Eu investigaria primeiro os **joins** (um `inner join` descarta linhas em silêncio), depois a deduplicação e as regras que filtram sem gravar na quarentena. Essa checagem pegou um problema real na Gold: o `inner join` com `silver.produtos` descartava os 652 itens dos 3 produtos em quarentena, e a receita da Gold ficou R$ 506.236,41 menor que a da Silver (notebook 04, seção 6.1).

**14. Por que os itens “na mão” (50.105) e “pela função” (50.030) diferem?**
A diferença é de **75 itens**, exatamente os itens dos **40 pedidos órfãos** (`cliente_id = 999999`). A função `build_silver` aplica a regra de integridade referencial **antes** de explodir: pedidos sem cliente vão para a quarentena e seus itens não entram. A versão “na mão” só deduplicou e explodiu, sem checar o cliente. Na checagem manual de produto, os itens foram comparados com `bronze.produtos` (200, incluindo os 3 com preço negativo), por isso deu `rejeitados: 0`.

## 4. Gold

**15. O que é o grão? Grão de cada tabela.**
Grão é **o que uma linha representa**, ou seja, a combinação de colunas que deve ser única. Na versão Spark:
- `vendas_diarias`: **data × categoria × estado × canal_aquisicao** (30.755 linhas).
- `clientes_360`: **um cliente** (`cliente_id`, 5.000 linhas = todos os clientes da Silver).
- `avaliacoes_resumo`: **mês × categoria** (112 linhas).

Se o grão for violado (ex.: duas linhas para o mesmo cliente), **somas e contagens duplicam**: a receita total e o número de clientes do painel inflam, e os joins com outras tabelas multiplicam linhas. O gate de grão pega isso: com uma linha repetida em `vendas_diarias`, a saída foi `grão: 1 linha por data x categoria x estado x canal_aquisicao  ERRO -> publicação BLOQUEADA`. Outro cuidado do grão: `qtd_pedidos` é distinto **dentro** do grupo, então somar `qtd_pedidos` de todas as linhas conta duas vezes um pedido com itens de categorias diferentes.

**16. Para que serve o contrato de dados?**
Ele diz **o que a tabela é e o que ela promete**: dono (a quem perguntar e quem responde por erros), grão (como agregar sem duplicar), descrição de cada coluna (o que é `receita`: inclui cancelados? E frete?) e se tem dado pessoal. Sem contrato, o mesmo número aparece diferente em dois painéis. Quem usa: **analistas e o pessoal de BI** (para montar perguntas certas no Metabase), **cientistas de dados e IA** (o que pode virar feature ou chunk), **o próprio pipeline** (os gates conferem se toda coluna tem descrição e se não há PII proibida) e **governança/auditoria**. Ele foi publicado em `s3://warehouse/lakehouse/gold/_contratos/contratos.json`. Exemplo: `clientes_360` → `"grao": "um cliente"`, `"pii": true`, `"nome_mascarado": "Nome com letras ocultas (A** S****)"`.

**17. Dois gates e por que bloquear é melhor que avisar.**
- **Reconciliação de receita:** `receita Gold = receita Silver`. Na Gold original deu **ERRO** (40.310.963,52 ≠ 40.817.199,93). Na corrigida, OK.
- **Nenhuma coluna de PII bruta:** a `clientes_360` original tinha a coluna `nome` e deu **ERRO**. Com `nome_mascarado`, OK.
- (Outros: grão único, chave sem nulos, toda coluna descrita no contrato, `nota_media` entre 1 e 5, todos os clientes da Silver presentes.)

Bloquear é melhor porque a Gold é **consumida automaticamente**: o Metabase mostra o número errado para a diretoria e ninguém lê um aviso no log. Um dado errado publicado gera decisões erradas e perda de confiança, e é pior que um dado **atrasado**. Com o bloqueio, a versão anterior (correta) continua no ar até o problema ser resolvido. No caso de PII, publicar e depois “avisar” já é o vazamento.

**18. Por que recalcular a receita “por outro caminho”?**
Porque um erro de **lógica** (join que perde linhas, filtro errado, duplicação por grão) produz números **plausíveis, sem nulos e sem tipos errados**, que passam em qualquer checagem de nulos ou de formato. Recalculando direto de `silver.itens_pedido` + `silver.pedidos` (sem passar por produtos e clientes), qualquer linha perdida ou duplicada no caminho da Gold aparece como diferença. Foi exatamente o que aconteceu: a Gold original não tinha nenhum nulo, mas faltavam **R$ 506.236,41 e 1.282 unidades** (652 itens dos produtos 55, 114 e 173, em quarentena, sumiram no `inner join`). A correção foi um `left join` com `categoria = 'nao_classificado'`. Depois disso, `receita Gold (40.817.199,93) = receita Silver (40.817.199,93)` e `itens 87.698 = 87.698`.

**19. Fonte da verdade e cópia. E se o Postgres for apagado?**
A **fonte da verdade é a Gold no Iceberg** (`s3://warehouse/lakehouse/gold/`, com histórico e contrato). O **Postgres é uma cópia de serviço**, porque o Metabase não lê Iceberg. As contagens batem: `gold.vendas_diarias 30.755 linhas no Postgres | Iceberg: 30.755`. Se o Postgres for apagado, **nenhum dado é perdido**: basta rodar a publicação de novo (`publish_to_postgres` para as 3 tabelas) e recriar o usuário `metabase_ro`. O que se perde temporariamente é o serviço (os painéis ficam fora do ar). Se as tabelas forem recriadas com outra estrutura, as perguntas do Metabase podem precisar de ajuste. O mesmo vale para o pgvector no próximo módulo.

## 5. Qualidade e observabilidade

**20. Limpar dado × medir qualidade. Por que guardar o DQ numa tabela?**
**Limpar** é transformar: corrigir, tipar, deduplicar, mandar para a quarentena (o e-mail inválido virou `NULL`). **Medir** é observar e registrar o estado do dado depois da limpeza, sem alterá-lo. O DQ diz que 270 clientes (5,4%) estão sem e-mail. Limpar sem medir esconde o problema. Medir sem limpar não protege o consumidor. Guardar o relatório numa tabela (`lake.silver.dq_resultados`) transforma qualidade em **dado consultável**: dá para ver tendência (o `pct_falha` do e-mail subiu?), criar alertas e dashboards, auditar (“qual era a qualidade no dia X?”) e cruzar com incidentes. É o começo da observabilidade.

**21. Quatro sinais do dia a dia e o que seria alerta.**
| Sinal | Valor normal no meu resultado | Alerta |
|---|---|---|
| **Contagem por camada / reconciliação** | 30.000 = 29.960 + 40; receita Gold = Silver | conta que não fecha, ou queda/salto de volume (> ±20% contra a média do lote) |
| **Tamanho da quarentena por origem** | 68 (40 órfãos, 25 notas, 3 produtos) | crescimento brusco (ex.: órfãos saltando de 40 para 4.000 indica que a carga de clientes falhou) ou origem nova aparecendo |
| **Resultados do DQ** | 7 OK, 1 AVISO (e-mail 5,4%) | qualquer ERRO; AVISO cujo `pct_falha` sobe execução a execução |
| **Frescor** | `max(_ingest_ts)` e `max(data_pedido)` do lote do dia | lote do dia não chegou na landing, ou a Gold não foi publicada no horário (gate bloqueou) |
| **Storage / snapshots** (bônus) | Bronze `pedidos`: 12 objetos para 1 arquivo ativo | número de objetos crescendo sem `expire_snapshots`, ou muitos arquivos pequenos |

**22. Vantagem de gravar um objeto novo a cada execução do DQ.**
O histórico fica **completo e imutável**: cada execução é um snapshot (no notebook 04, `dq_resultados` tem 4 execuções, de 2026-11-05 22:26 a 2026-11-21 06:29, 32 linhas e 10 snapshots `append`). Isso permite: (a) ver a **série temporal** da qualidade (o e-mail esteve sempre em 5,4%, então é sujeira estável e não degradação); (b) comparar antes e depois de uma mudança no pipeline; (c) auditar o que se sabia em cada data. Sobrescrevendo, só existiria “a última foto”, e uma degradação lenta passaria despercebida.

## 6. Versões, histórico e recuperação

**23. Pedidos cancelados apagados por engano: como descobrir, voltar e desfazer?**
**Iceberg (`02_silver`):** antes do erro, guardamos o snapshot bom (`6192292304794414773`). O `DELETE … WHERE status = 'cancelado'` mudou a contagem: `linhas: 50105 -> 43886 (sumiram 6219)`. A tabela `.snapshots` mostra **o que mudou e quando** (operação `delete`, `deleted-records`). Para comparar, `VERSION AS OF 6192292304794414773` = **50.105** contra a versão atual **43.886**. Para desfazer, `CALL lake.system.rollback_to_snapshot('silver.itens_pedido_aula', 6192292304794414773)` → **50.105**.
**Versionamento do bucket (`04`, seção 4):** num bucket `historico` com versionamento ligado, gravamos o Parquet correto e depois o “limpo”. `list_object_versions` mostrou 2 versões (916.263 bytes contra 799.503 bytes). Lemos a versão boa por `VersionId` (50.105 linhas) e a restauramos com `copy_object(…, VersionId=boa)`, que virou uma 3ª versão com 50.105 linhas.
**Diferença:** o Iceberg versiona a **tabela inteira** (vários arquivos de forma atômica e consistente, com SQL, time travel e rollback, sabendo o que é uma linha ou um snapshot). O versionamento do bucket versiona **cada objeto isoladamente**, sem saber o que é uma tabela: para uma tabela com 100 arquivos seria preciso restaurar cada um e torcer para ficarem coerentes entre si.

**24. Por que o dado antigo continuava existindo depois do DELETE? E a LGPD?**
Porque no object storage nada é editado: o `DELETE` do Iceberg só criou um **snapshot novo** que deixa de apontar para os arquivos antigos, e os Parquet antigos (com os 6.219 itens cancelados) continuam no bucket, acessíveis por `VERSION AS OF`. No bucket versionado, `delete_object` criou apenas uma **marca de exclusão**: `versões com dado ainda guardadas: 3 | marcas de exclusão: 1`. O **conflito com a LGPD**: quando um titular pede para ser esquecido, apagar a linha não basta, porque o dado pessoal segue nos snapshots e versões. **Solução:** no Iceberg, `DELETE` + `CALL system.expire_snapshots(...)` (remove snapshots antigos e seus arquivos) + `remove_orphan_files`. No bucket, apagar **cada `VersionId`** (no notebook: `0 versões, 0 marcas` depois disso) e configurar regras de ciclo de vida (`NoncurrentVersionExpiration`). Também é preciso uma **política de retenção** que equilibre auditoria e esquecimento (ex.: 30 dias de histórico), e, de preferência, minimizar a PII já na Bronze ou isolá-la numa tabela separada, mais fácil de expurgar.

## 7. Segurança, privacidade e segredos

**25. Nome mascarado e sem e-mail na `clientes_360`: que princípio?**
**Minimização e necessidade** (LGPD, art. 6º, III): só deve ser tratado o dado necessário para a finalidade. Quem analisa “quanto o cliente comprou” no Metabase não precisa do nome completo nem do e-mail. O resultado foi `Aline Pereira` → `A**** P******`, sem a coluna `email`, e um gate impede publicar `nome`, `email`, `cpf` ou `telefone`. Junta-se a isso o **menor privilégio**: os dados completos ficam na Silver, com acesso restrito. **Outro campo que eu trataria:** o `cliente_id`, que é um pseudônimo e permite religar à pessoa cruzando com a Silver. Na Gold de BI ele poderia virar um hash com salt (`sha256(cliente_id || salt)`). Também a combinação `estado` + `data_cadastro` + `canal_aquisicao`, que pode reidentificar clientes em UFs com poucos registros (generalizar `data_cadastro` para mês/ano). E o **texto livre das avaliações**, que pode conter telefone ou e-mail e precisa da máscara de PII antes de virar chunk ou embedding.

**26. Por que o Metabase deve usar um usuário somente leitura? Qual o teste?**
**Menor privilégio:** o Metabase permite SQL nativo e é usado por muita gente. Com um usuário que escreve, um erro ou uma credencial vazada poderia apagar ou alterar a Gold, ou criar tabelas. Quem faz BI só precisa ler. O `metabase_ro` recebeu só `CONNECT`, `USAGE ON SCHEMA gold` e `SELECT` (mais `ALTER DEFAULT PRIVILEGES` para tabelas futuras). **Teste (notebook 04, seção 8.1)**, conectado como `metabase_ro`:
```
SELECT        PERMITIDO  [(30755,)]
INSERT        NEGADO     permission denied for table avaliacoes_resumo
DELETE        NEGADO     permission denied for table vendas_diarias
TRUNCATE      NEGADO     permission denied for table clientes_360
CREATE TABLE  NEGADO     permission denied for schema gold
DROP TABLE    NEGADO     must be owner of table vendas_diarias
```
O teste prova as duas metades: ele **consegue** ler (senão o painel não funcionaria) e **não consegue** alterar nada.

**27. Senhas no código (`admin12345`, `aula123`): por que aqui sim e em produção não?**
Aqui é aceitável porque o ambiente é **local, descartável e sem dado real**: roda na máquina do aluno, os dados são sintéticos e o objetivo é didático (todos precisam do mesmo valor para a aula funcionar). Mesmo assim, o `docker/.env` está no `.gitignore` e o código lê de variáveis de ambiente (`MINIO_PASSWORD`, `PG_PASSWORD`), deixando a senha só como padrão. Em produção seria inaceitável: senha no código vai para o Git e para o histórico (para sempre), é compartilhada por todos, não tem rotação nem auditoria, e um vazamento do repositório expõe todo o lake. Em produção: **cofre de segredos** (HashiCorp Vault, AWS Secrets Manager, Azure Key Vault, Kubernetes Secrets com criptografia), injetado em tempo de execução; **identidades por serviço** (IAM roles / service accounts, credenciais temporárias, sem chave fixa); **rotação automática**; uma credencial por etapa com o mínimo de permissão; TLS em todas as conexões; e varredura de segredos no CI (gitleaks).

**28. Por que a carga no Postgres usa TRUNCATE e não recria a tabela?**
O `TRUNCATE` **mantém o objeto tabela**: mesmo OID, colunas, tipos, `GRANT`s, índices e dependências. Ele só esvazia as linhas antes de inserir as novas. Recriando (`DROP` + `CREATE`, o padrão do `mode=overwrite` sem `truncate=true`), quebraria no Metabase: (a) **permissões**: o `GRANT SELECT` para `metabase_ro` some com a tabela (a menos que existam default privileges), e todos os painéis dão “permission denied”; (b) **metadados do Metabase**: ele guarda IDs de tabela e campo, tipos semânticos, descrições e remapeamentos. Uma tabela recriada pode ganhar campos novos no sync, e perguntas e filtros que apontavam para os antigos quebram; (c) **tipos**: o Spark pode recriar colunas com tipos diferentes (`TEXT` em vez de `DATE`) e mudar o comportamento dos gráficos; (d) a janela entre `DROP` e `CREATE` deixa o painel com “tabela não existe”. Vimos isso na prática: a `clientes_360` precisou ser recriada **uma vez** porque o esquema mudou (`nome` → `nome_mascarado`). Por isso o notebook reaplica o `GRANT`, e uma pergunta que usasse a coluna `nome` teria de ser refeita. Nas cargas seguintes volta o TRUNCATE.

## 8. Visão de arquitetura

**29. Fluxo completo, da landing ao Metabase.**
| # | Etapa | Formato | Onde fica | Quem pode ler (e quem escreve) |
|---|---|---|---|---|
| 1 | Origem → **Landing** | CSV (`clientes`, `produtos`, `avaliacoes`) e JSONL (`pedidos`) | MinIO, bucket `landing`, `mercadonordeste/lote1/` | lê: job de ingestão · escreve: sistema de origem |
| 2 | **Bronze** | Iceberg (Parquet + metadata), particionada por `_batch_id` | bucket `warehouse`, `lakehouse/bronze/<tabela>/` | lê: job da Silver, auditoria · escreve: job de ingestão |
| 3 | **Silver** + **quarentena** + **dq_resultados** | Iceberg | `warehouse/lakehouse/silver/…` | lê: job da Gold, time de dados · escreve: job da Silver / DQ |
| 4 | **Gold** + **contrato** | Iceberg + JSON | `warehouse/lakehouse/gold/…` e `gold/_contratos/contratos.json` | lê: job de publicação, analistas, IA · escreve: job da Gold (só com gates OK) |
| 5 | **Serving** | tabelas relacionais | Postgres `serving`, schema `gold` | lê: `metabase_ro` (SELECT) · escreve: `aula` (TRUNCATE + INSERT) |
| 6 | **Metabase** | perguntas e dashboards | `https://localhost:3443` (Caddy → Metabase) | usuários de negócio |

O diagrama completo, em Mermaid, está em `entrega/ATIVIDADE_ARQUITETURA_MEDALHAO.md`.

**30. Volume 1.000× maior: o que mudar primeiro?**
**O catálogo, primeiro.** É a única peça que fica **incorreta** (e não apenas lenta) em escala. O Hadoop catalog controla a versão atual pelo arquivo `version-hint.text` e depende de rename atômico, que o S3/MinIO não oferece. Com escrita concorrente (vários jobs ou lotes em paralelo, que é o que acontece quando o volume cresce), dois commits podem se sobrescrever e corromper a tabela. Eu trocaria por um catálogo com commit transacional: REST Catalog (Polaris/Gravitino), Nessie, AWS Glue ou Hive Metastore. **Depois, o processamento:** o `local[2]` com 2 GB não processa 30 milhões de pedidos em tempo útil. Ele iria para um cluster (Spark em Kubernetes, EMR, Databricks), e a Silver passaria a ser **incremental** (`MERGE INTO` só do lote novo), porque hoje ela reconstrói toda a Bronze a cada execução. **O formato por último:** Parquet + Iceberg já é o formato de escala (colunar, compressão, pruning por estatísticas). Bastaria ajustar o particionamento (por data do pedido, e não só por `_batch_id`), compactar arquivos pequenos (`rewrite_data_files`) e expirar snapshots (já com 1 lote reexecutado, a Bronze de pedidos foi de 5 para 12 objetos).

---

## Pergunta aberta sobre IA

> Rascunho a partir do que aconteceu nesta entrega. Ajuste com a sua experiência.

Usei uma IA (Claude Code) para montar o notebook `04_governanca_spark.ipynb`, levando para Spark os gates e o contrato que só existiam na versão pandas. Três coisas que ela propôs foram **confirmadas rodando o código**:
1. **Diagnóstico de ambiente:** o Spark não abria (`getSubject is not supported`). A IA identificou que o Java padrão da máquina tinha passado a ser o 25 e apontou o JDK 17 do SDKMAN. Depois o worker falhou com `PYTHON_VERSION_MISMATCH (3.15 vs 3.12)`, resolvido fixando `PYSPARK_PYTHON`. Confirmei porque a sessão subiu e os jobs rodaram.
2. **Erro de reconciliação na Gold:** a IA sugeriu recalcular a receita direto da Silver. O gate deu ERRO (R$ 40.310.963,52 contra R$ 40.817.199,93), e ela atribuiu a diferença aos produtos em quarentena. **Não aceitei a explicação sem prova:** a célula de diagnóstico mostrou os produtos 55, 114 e 173 com 652 itens e R$ 506.236,41, exatamente a diferença. Depois do `left join`, o gate fechou no centavo.
3. **Usuário somente leitura:** confirmei pelo teste de permissões (SELECT permitido, INSERT/DELETE/TRUNCATE/CREATE/DROP negados).

**Descartei:** o uso de `toPandas()` no experimento do versionamento do bucket, que falhou no Python 3.12 (`No module named 'distutils'`) e foi trocado por `collect()`. **Como sei que estava certa:** não pelo texto da IA, mas porque cada afirmação virou uma checagem executável com saída gravada no notebook (contagens, somas que batem no centavo, permissões negadas pelo próprio Postgres).
