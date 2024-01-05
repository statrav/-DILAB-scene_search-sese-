from transformers import T5Tokenizer, T5ForConditionalGeneration, pipeline, AutoModelForCausalLM, AutoTokenizer
import pandas as pd
import torch
import re
from peft import PeftModel, PeftConfig
import os

#### caption (complex sentence) to simple sentence ####

checkpoint="unikei/t5-base-split-and-rephrase"
tokenizer = T5Tokenizer.from_pretrained(checkpoint)
model = T5ForConditionalGeneration.from_pretrained(checkpoint)

data_path = '../data/raw'
file_name = os.listdir(data_path)
file_path = data_path + '/' + file_name[0]
df = pd.read_csv(file_path)

result = []

for idx in df.index():
    value = df.at[idx, 'captions']
    complex_tokenized = tokenizer(value,
                                  padding = 'max_length',
                                  truncation = True,
                                  max_length = 256,
                                  return_tensors = 'pt')
    
    simple_tokenized = model.generate(complex_tokenized['input_ids'], 
                                      attention_mask = complex_tokenized['attention_mask'], 
                                      max_length=256, 
                                      num_beams=5)
    simple_sentences = tokenizer.batch_decode(simple_tokenized, skip_special_tokens=True)
    
    result.append(simple_sentences)
    
    
df2 = pd.DataFrame({'simple_sents': result})
df = pd.concat([df, df2], axis=1)

def ss_processing(simple_sents):
    simple_sents = simple_sents[2:-2]
    sents = simple_sents.split('. ')
    if sents[0] != '.':
        sents[0] = sents[0] + '.'
    return sents

sents = df['simple_sents'].apply(lambda x : pd.Series(ss_processing(x)))
sents = sents.stack().reset_index(level=1, drop=True).to_frame('simple_sent')

df = pd.merge(df, sents, left_index = True, right_index = True, how = 'left')


#### simple sentence to scene graph ####

peft_model_id = 'DIlabmasters/etri-llm-v1'
config = PeftConfig.from_pretrained(peft_model_id)
model = AutoModelForCausalLM.from_pretrained(config.base_model_name_or_path,
                                             return_dict = True, device_map = 'auto')
tokenizer = AutoTokenizer.from_pretrained(config.base_model_name_or_path)

model = PeftModel.from_pretrained(model, peft_model_id)

model.eval()

def ask(x, context = '', is_input_full = False):
    x = '### Question: Translate this sentence into a scene graph. </s> \n  <s> ### Sentence: ' + x + '</s> <s> ### Answer:'
    batch = tokenizer(x, return_tensors='pt')
    
    batch = batch.to('cuda')
    
    with torch.cuda.amp.autocast():
        output_tokens = model.generate(**batch, max_length=200)
    
    output = tokenizer.decode(output_tokens[0], skip_special_tokens=True)
    
    print('\n\n', output)
    return output


result = []

for idx in df.index:
    x = df.at[idx, 'simple_sent']
    output = ask(x)
    result.append(output)
    print(idx, 'completed')
    
df2 = pd.DataFrame({'scene_graphs' : result})
df = pd.concat([df, df2], axis=1)

def sg_split(x):
    idx = x.find('### Answer: ')
    idx = idx + 11
    x = x[idx:]
    x = x.split(') , (')
    return x

sgs = df['scene_graphs']
sgs = sgs.apply(sg_split)


sgs = sgs.apply(lambda x: pd.Series(x))

sgs = sgs.stack().reset_index(level=1, drop=True).to_frame('scene_graph')

df = df.merge(sgs, left_index=True, right_index=True, how='left')
df.reset_index(inplace = True)

s_list = []
p_list = []
o_list = []


def sg_clean(x):
    x = re.sub(r'[^\w\s]', '', x)
    return x.strip()

for idx in df.index:
    sg = df.at[idx, 'scene_graph']
    
    if len(sg.split(',')) == 3:
        s, p, o = sg.split(',')
        s = sg_clean(s)
        p = sg_clean(p)
        o = sg_clean(o)
        
        s_list.append(s)
        p_list.append(p)
        o_list.append(o)
        
    elif len(sg.split(',')) == 2:
        s, p = sg.split(',')
        s = sg_clean(s)
        p = sg_clean(p)
        
        s_list.append(s)
        p_list.append(p)
        o_list.append(None)
        
    else:
        s_list.append(None)
        p_list.append(None)
        o_list.append(None)


df2 = pd.DataFrame({'subject': s_list, 'predicate': p_list, 'object' : o_list})
df = pd.concat([df, df2], axis=1)
df.drop_duplicates(['subject', 'predicate', 'object'], inplace=True, ignore_index=True)
df['begin_frame'] = df['begin_frame'].apply(lambda x: round(x, 2))
df['end_frame'] = df['end_frame'].apply(lambda x: round(x, 2))

# df = df[['index', 'video_id', 'video_path', 'duration', 'begin_frame', 'end_frame', 'captions', 'subject', 'predicate', 'object']]

df.to_csv('../data/scene_graph.csv')
