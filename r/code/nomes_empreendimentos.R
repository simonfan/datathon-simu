######   1. PACOTES E DIRETORIOS      ######
pacotes <- c("dplyr", "ggplot2", "readxl","tibble", "tidytext", "tidyverse", "stringr", "textstem")


if(sum(as.numeric(!pacotes %in% installed.packages())) != 0){
  instalador <- pacotes[!pacotes %in% installed.packages()]
  for(i in 1:length(instalador)) {
    install.packages(instalador, dependencies = T)
    break()}
  sapply(pacotes, require, character = T) 
} else {
  sapply(pacotes, require, character = T) 
}


# Definindo o local do script como diretório
setwd(dirname(rstudioapi::getSourceEditorContext()$path))
getwd()


######   2. BASE      ######

# Base empreendimentos
df_base_empreend <- read.csv(file.path("..",
                                       "data",
                                       "simu-carteira-mun-T.csv")) |> 
  filter(situacao_obra_mdr == "CONCLUÍDA") |> 
#  select(acao,  empreendimento)
##  |>   filter(acao == '8487')
  select(empreendimento)

# Dicionário ações
dic_acao <- read.csv2(file.path("..",
                               "data",
                               "ações_orçamentárias_simu.csv")) 


??lemmatize_words                                           # transformar em singular
  

######   3. TRATAMENTO NOME EMPREENDIMENTOS      ######

# Retirar Acentos
empreend_ajust <- df_base_empreend %>%
  mutate(empreendimento = str_to_lower(empreendimento),                            # Todos em minúsculo
         empreendimento = str_replace_all(empreendimento, "_", " "),               # Trocar _ por espaço
         empreendimento = str_replace_all(empreendimento, "\\.", " "),             # Trocar pontos por espaço
         empreendimento = str_replace_all(empreendimento, "aa", "a"),               # 2 "a" colados viram 1 "a"
         empreendimento = str_replace_all(empreendimento, "xx", " "),               # 2 "x" colados viram 1 espaco     
         empreendimento = iconv(empreendimento, to = "ASCII//TRANSLIT"),            # Remover acentos
         empreendimento = str_replace_all(empreendimento, "[0-9]", ""),            # Remover números
         empreendimento = str_replace_all(empreendimento, "sinalizacao", "sinalizacao "),    # Ajustar sinalizacao
         empreendimento = str_replace_all(empreendimento, "\\s+", " "),            # Transformar espaços em um único espaço
         empreendimento = str_trim(empreendimento)) %>%                            # Remover espaços antes e depois
  unnest_tokens(word, empreendimento) %>%
  filter(str_length(word) > 3) %>%                                                # Eliminar palavras com até tres letras
  anti_join(get_stopwords(language = 'pt'), by = "word")                          # Remover stopwords




# lemmatizacao
# fonte: https://marcusnunes.me/posts/analise-de-sentimentos-com-r-bojack-horseman-vs-brooklyn-99/
# obs - colocar no formato tidy antes de executar.

lemma_dic <- read.delim(file = "https://raw.githubusercontent.com/michmech/lemmatization-lists/master/lemmatization-pt.txt", header = FALSE, stringsAsFactors = FALSE)
names(lemma_dic) <- c("stem", "term")


lemma_dic_ajustado <- lemma_dic |>
  filter(!stem == "ver") |> 
  mutate(term = str_to_lower(term),                           # todos em minúsculo
         term = iconv(term, to = "ASCII//TRANSLIT"),           # tirar acentos
         stem = str_to_lower(stem),                           # todos em minúsculo
         stem = iconv(stem, to = "ASCII//TRANSLIT")) |>            # tirar acentos 
  distinct(term, .keep_all = TRUE)



# Realiza o left join
empreend_lemma <- left_join(empreend_ajust, lemma_dic_ajustado, by = c("word" = "term"))|> 
  mutate(tratado = ifelse(is.na(stem), word, stem))



# Contar Palavras
empreend_cont <- empreend_lemma %>% select(tratado) %>% count(tratado, sort = TRUE)



