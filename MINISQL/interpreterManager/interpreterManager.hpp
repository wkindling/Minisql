#ifndef _INTERPRETERMANAGER_H_
#define _INTERPRETERMANAGER_H_
#include "interpreter.h"
#include "string.h"
interpreterManager::interpreterManager()
{
	operation=UNDEFINED;
	oTableName="";
	oIndexName="";
	oAttributeName="";
	primaryKeyIndex=-1;
	uniqueKeyIndex=-1;
}

void interpreterManager::initialize()
{
	operation=UNDEFINED;
	oTableName="";
	oIndexName="";
	oAttributeName="";
	primaryKeyIndex=-1;
	uniqueKeyIndex=-1;
	if (oAttributeList.size()>0) oAttributeList.clear();
	if (oConditionList.size()>0) oConditionList.clear();
	if (oTuple.attributeValueList.size()>0) oTuple.attributeValueList.clear();
}

bool interpreterManager::getString(string& input, string &output)
{
	unsigned int position=0;
	output.clear();
	char temp;
	if (input[0]=='\'') 
	{
		output="";
		return true;
	}
	else 
	{
		for (position=0;position<input.length();position++)
		{
			if(input[position]!='\'')
			{
				temp=input[position];
				output+=temp;
			}
			else
			{
				input.erase(0,position);
				return true;
			}
		}
	}
	return false;
}

bool interpreterManager::isInt(const char* input)
{
	int length=strlen(input);
	if (!isdigit(input[0])&&!(input[0]=='-')) return false;
	for (int i = 1; i < length; i++)
		if (!isdigit(input[i])) return false;
	return true;
}

bool interpreterManager::isFloat(const char* input)
{
	int length=strlen(input);
	int dotNum=0;
	if (!isdigit(input[0])&&!(input[0]=='-')) return false;
	for (int i=1;i<length;i++)
	{
		if (!isdigit(input[i])&&input[i]!='.') return false;
		else 
		{
			if (input[i]=='.')
			{
				if (dotNum==0) 
				{
					dotNum++;
				}
				else 
				{
					return false;
				}
			}

		}

	}
	return true;
}

bool interpreterManager::getToken(string & command, string& result)
{
	result.clear();
	unsigned int commandPosition=0;
	unsigned int resultPosition=0;
	char temp=' ';

	for (;commandPosition<command.length();commandPosition++)
	{
		if (temp==' '||temp=='\t'||temp==10||temp==13) temp=command[commandPosition];
		else break;
	}
	if (commandPosition==command.length()&&(temp==' '||temp=='\t'||temp==10||temp==13)) return false;

	switch(temp)
	{
		case ',':
		case '(':
		case ')':
		case '*':
		case '=':
		case '\'':
			result+=temp;
			command.erase(0,commandPosition);
			break;
		case '<':
			result+=temp;
			temp=command[commandPosition++];
			if (temp=='='||temp=='>')
			{
				result+=temp;
				command.erase(0,commandPosition);
			}
			else
			{
				command.erase(0,commandPosition-1);
			}
			break;
		case '>':
			result+=temp;
			temp=command[commandPosition++];
			if(temp=='=')
			{
				result+=temp;
				command.erase(0,commandPosition);
			}
			else
			{
				command.erase(0,commandPosition-1);
			}
			break;
		default:
			do
			{
				result+=temp;
				if (commandPosition<command.length()) temp=command[commandPosition++];
				else
				{
					command.erase(0,commandPosition);
					result[resultPosition++]='\0';
					return true;
				}
			}while (temp != '*' && temp != ',' && temp != '(' && temp != ')'
				&& temp != ' ' && temp != '\t' && temp != '=' && temp != '>' 
				&& temp != '<' && temp != '\'' && temp != 10 && temp != 13);
			command.erase(0,commandPosition-1);
	}
	return true;
}

void interpreterManager::parseCommand(char* command)
{
	string word="";
	string temp=command;
	int findPrimaryKey=0,findUniqueKey=0;
	Attribute tempAttribute;
	Condition tempCondition;
	string tempTuple;
	bool flag;
	initialize();

	flag=getToken(temp,word);
	if (!flag){operation=EMPTY;return;}
	/*------------exefile--------------*/
	if (strcmp(word.c_str(),"exefile")==0)
	{
		operation=EXEERROR;
		flag=getToken(temp,word);
		if (!flag) return;
		exeFileName=word;
		operation=EXEFILE;
		return;
	}
	/*------------quit--------------*/
	if (strcmp(word.c_str(),"quit")==0)
	{
		flag=getToken(temp,word);
		if (!flag) operation=QUIT;                   
		return;
	}
	/*------------select--------------*/
	if (strcmp(word.c_str(),"select")==0) 
	{
		operation=SELECTERROR;                       
		flag=getToken(temp,word);
		if (!flag) return;
		tempAttribute.attributeName=word;
		oAttributeList.push_back(tempAttribute);
		flag=getToken(temp,word);
		if (!flag) return;
		while (strcmp(word.c_str(),",")==0)  
		{
			flag=getToken(temp,word);
			if (!flag) return;
			tempAttribute.attributeName=word;
			oAttributeList.push_back(tempAttribute);
			flag=getToken(temp,word);
			if (!flag) return;
		}
		//select... from...
		if (strcmp(word.c_str(),"from")!=0) return; 
		flag=getToken(temp,word);
		if (!flag) return;
		oTableName=word;
		if (!catalogManager.tableCheck(oTableName))
		{
			operation=TABLENOTEXIST;             
			return;
		}
		oTable=catalogManager.getTable(oTableName);
		flag=getToken(temp,word);
		if (!flag) 
		{
			operation=SELECTALL;                 
			return;
		}
		//select ... from ... where ...
		if (strcmp(word.c_str(),"where")!=0) return;
		flag=getToken(temp,word);
		if (!flag) {operation=SELECTERROR;}
		tempCondition.attributeIndex=catalogManager.getAttributeIndex(oTable,word);
		tempCondition.tableName=oTableName;
		oAttributeName=word;
		if (tempCondition.attributeIndex==-1)
		{
			operation=ATTRIBUTEERROR;             
			return;
		}
		flag=getToken(temp,word);
		if (!flag) return;
		if (strcmp(word.c_str(),"<")==0) tempCondition.compareOp=lessThan;
		else if (strcmp(word.c_str(),"<=")==0) tempCondition.compareOp=lessEqual;
		else if (strcmp(word.c_str(),"=")==0) tempCondition.compareOp=equalTo;
		else if (strcmp(word.c_str(),">=")==0) tempCondition.compareOp=largerEqual;
		else if (strcmp(word.c_str(),">")==0) tempCondition.compareOp=largerThan;
		else if (strcmp(word.c_str(),"<>")==0) tempCondition.compareOp=notEqual;
		else return;
		flag=getToken(temp,word);
		if (!flag) return;
		if (strcmp(word.c_str(),"\'")==0)
		{
			flag=getString(temp,word);
			if (!flag) return;
			tempCondition.conditionValue=word;
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"\'")!=0) return;
		}
		else tempCondition.conditionValue=word;
		oConditionList.push_back(tempCondition);
		flag=getToken(temp,word);
		if (!flag)
		{
			operation=SELECTCONDITION;
			return;
		}
		while (strcmp(word.c_str(),"and")==0)
		{
			flag=getToken(temp,word);
			if (!flag) return;
			tempCondition.attributeIndex=catalogManager.getAttributeIndex(oTable,word);
			if (tempCondition.attributeIndex==-1)
			{
				operation=ATTRIBUTEERROR;
				return;
			}
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"<")==0) tempCondition.compareOp=lessThan;
			else if (strcmp(word.c_str(),"<=")==0) tempCondition.compareOp=lessEqual;
			else if (strcmp(word.c_str(),"=")==0) tempCondition.compareOp=equalTo;
			else if (strcmp(word.c_str(),">=")==0) tempCondition.compareOp=largerEqual;
			else if (strcmp(word.c_str(),">")==0) tempCondition.compareOp=largerThan;
			else if (strcmp(word.c_str(),"<>")==0) tempCondition.compareOp=notEqual;
			else return;
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"\'")==0)
			{
				flag=getString(temp,word);
				if (!flag) return;
				tempCondition.conditionValue=word;
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),"\'")!=0) return;
			}
			else tempCondition.conditionValue=word;
			oConditionList.push_back(tempCondition);
			flag=getToken(temp,word);
			if (!flag)
			{
				operation=SELECTCONDITION;
				return;
			}		
		}
	}
	/*------------create--------------*/
	if (strcmp(word.c_str(),"create")==0)
	{
		flag=getToken(temp,word);
		if (!flag) return;
		/*------------create table--------------*/
		if (strcmp(word.c_str(),"table")==0)
		{
			operation=CREATETABLEERROR;
			flag=getToken(temp,word);
			if (!flag) return;
			oTableName=word;
			if (catalogManager.tableCheck(oTableName))
			{
				operation=TABLEEXISTED;
				return;
			}
			oTable.tableName=word;
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"(")!=0) return;
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"unique")==0||strcmp(word.c_str(),"primary")==0) return;
			tempAttribute.attributeName=word;
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"int")==0)
			{
				tempAttribute.attributeType=INTTYPE;
				tempAttribute.attributeLength=INTLENGTH;
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),"unique")==0) 
				{
					tempAttribute.isUnique=true;
					flag=getToken(temp,word);
					if (!flag) return;
				}
			}
			else if (strcmp(word.c_str(),"float")==0)
			{
				tempAttribute.attributeType=FLOATTYPE;
				tempAttribute.attributeLength=FLOATLENGTH;
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),"unique")==0) 
				{
					tempAttribute.isUnique=true;
					flag=getToken(temp,word);
					if (!flag) return;
				}
			}
			else if (strcmp(word.c_str(),"char")==0)
			{
				tempAttribute.attributeType=CHARTYPE;
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),"(")!=0) return;
				flag=getToken(temp,word);
				if (!flag) return;
				if (!isInt(word.c_str())) return;
				tempAttribute.attributeLength=atoi(word.c_str())+1;
				if (tempAttribute.attributeLength>256||tempAttribute.attributeLength<2)
				{
					operation=CHARLENGTHERROR;
					return;
				}
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),")")!=0) return;
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),"unique")==0)
				{
					tempAttribute.isUnique=true;
					flag=getToken(temp,word);
					if (!flag) return;
				}
			}
			else return;
			oTable.attributeList.push_back(tempAttribute);
			tempAttribute.isUnique=tempAttribute.isPrimaryKey=false;
			while (strcmp(word.c_str(),",")==0)
			{
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),"primary")==0)
				{
					flag=getToken(temp,word);
					if (!flag) return;
					if (strcmp(word.c_str(),"key")!=0) return;
					flag=getToken(temp,word);
					if (!flag) return;
					if (strcmp(word.c_str(),"(")!=0) return;
					flag=getToken(temp,word);
					if (!flag) return;
					for (unsigned int i=0;i<oTable.attributeList.size();i++)
					{
						if (strcmp(oTable.attributeList[i].attributeName.c_str(),word.c_str())==0)
						{
							findPrimaryKey=1;
							oTable.attributeList[i].isPrimaryKey=true;
							oTable.attributeList[i].isUnique=true;
						}
					}
					if (!findPrimaryKey)
					{
						operation=UNVALIDPRIMARYKEY;
						return;
					}
					findPrimaryKey=0;
					flag=getToken(temp,word);
					if (!flag) return;
					if (strcmp(word.c_str(),")")!=0) return;
					flag=getToken(temp,word);
					if (!flag) return;
				}
				else if (strcmp(word.c_str(),"unique")==0)
				{
					flag=getToken(temp,word);
					if (!flag) return;
					if(strcmp(word.c_str(),"(")!=0) return;
					flag=getToken(temp,word);
					if (!flag) return;
					for (unsigned int i=0;i<oTable.attributeList.size();i++)
					{
						if (strcmp(oTable.attributeList[i].attributeName.c_str(),word.c_str())==0)
						{
							findUniqueKey=1;
							oTable.attributeList[i].isUnique=true;
						}
					}
					if (!findUniqueKey)
					{
						operation=UNVALIDUNIQUE;
						return;
					}
					findUniqueKey=0;
					flag=getToken(temp,word);
					if (!flag) return;
					if(strcmp(word.c_str(),")")!=0) return;
					flag=getToken(temp,word);
					if (!flag) return;
				}
				else
				{
					tempAttribute.attributeName=word;
					flag=getToken(temp,word);
					if (!flag) return;
					if (strcmp(word.c_str(),"int")==0)
					{
						tempAttribute.attributeType=INTTYPE;
						tempAttribute.attributeLength=INTLENGTH;
						flag=getToken(temp,word);
						if (!flag) return;
						if (strcmp(word.c_str(),"unique")==0) 
						{
							tempAttribute.isUnique=true;
							flag=getToken(temp,word);
							if (!flag) return;
						}
					}
					else if (strcmp(word.c_str(),"float")==0)
					{
						tempAttribute.attributeType=FLOATTYPE;
						tempAttribute.attributeLength=FLOATLENGTH;
						flag=getToken(temp,word);
						if (!flag) return;
						if (strcmp(word.c_str(),"unique")==0) 
						{
							tempAttribute.isUnique=true;
							flag=getToken(temp,word);
							if (!flag) return;
						}
					}
					else if (strcmp(word.c_str(),"char")==0)
					{
						tempAttribute.attributeType=CHARTYPE;
						flag=getToken(temp,word);
						if (!flag) return;
						if (strcmp(word.c_str(),"(")!=0) return;
						flag=getToken(temp,word);
						if (!flag) return;
						if (!isInt(word.c_str())) return;
						tempAttribute.attributeLength=atoi(word.c_str())+1;
						if (tempAttribute.attributeLength>255||tempAttribute.attributeLength<1)
						{
							operation=CHARLENGTHERROR;
							return;
						}
						flag=getToken(temp,word);
						if (!flag) return;
						if (strcmp(word.c_str(),")")!=0) return;
						flag=getToken(temp,word);
						if (!flag) return;
						if (strcmp(word.c_str(),"unique")==0)
						{
							tempAttribute.isUnique=true;
							flag=getToken(temp,word);
							if (!flag) return;
						}
					}
					else return;
					oTable.attributeList.push_back(tempAttribute);
					tempAttribute.isPrimaryKey=tempAttribute.isUnique=false;
				}
			}
			if (strcmp(word.c_str(),")")!=0) return;
			flag=getToken(temp,word);
			if (!flag) operation=CREATETABLE;
		}
		/*------------create index--------------*/
		else if (strcmp(word.c_str(),"index")==0)
		{
			operation=CREATEINDEXERROR;
			flag=getToken(temp,word);
			if (!flag) return;
			oIndexName=word;
			oIndex.indexName=word;
			if (catalogManager.indexCheck(oIndexName))
			{
				operation=INDEXEXISTED;
				return;
			}
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"on")!=0) return;
			flag=getToken(temp,word);
			if (!flag) return;
			oTableName=word;
			if(!catalogManager.tableCheck(oTableName))
			{
				operation=TABLENOTEXIST;
				return;
			}
			oTable=catalogManager.getTable(oTableName);
			oIndex.tableName=word;
			flag=getToken(temp,word);
			if (!flag) return;	
			if (strcmp(word.c_str(),"(")!=0) return;
			flag=getToken(temp,word);
			if (!flag) return;
			int tempint;
			tempint=catalogManager.getAttributeIndex(oTable,word);
			if (tempint==-1) 
			{
				operation=ATTRIBUTEERROR;
				return;
			}
			if (catalogManager.indexCheck(oTableName))
			{
				operation=INDEXEXISTED;
				return;
			}
			oIndex.attributeIndex=tempint;
			oIndex.indexBlockNum=0;
			oIndex.attributeLength=oTable.attributeList[tempint].attributeLength;
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),")")!=0) return;
			flag=getToken(temp,word);
			if (!flag) operation=CREATEINDEX;
		}
	}
	/*------------delete--------------*/
	if (strcmp(word.c_str(),"delete")==0)
	{
		flag=getToken(temp,word);
		if(!flag) return;
		//delete... from ...
		if (strcmp(word.c_str(),"from")==0)
		{
			operation=DELETEERROR;
			flag=getToken(temp,word);
			if(!flag) return;
			oTableName=word;
			if (!catalogManager.tableCheck(word))
			{
				operation=TABLENOTEXIST;
				return;
			}
			flag=getToken(temp,word);
			oTable=catalogManager.getTable(oTableName);
			if (!flag)
			{
				operation=DELETEALL;
				return;
			}
			//delete from where
			if (strcmp(word.c_str(),"where")!=0) return;
			flag=getToken(temp,word);
			if (!flag) return;
			int tempint;
			tempint=catalogManager.getAttributeIndex(oTable,word);
			if (tempint==-1)
			{
				operation=ATTRIBUTEERROR;
				return;
			}
			tempCondition.attributeIndex=tempint;
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"<")==0) tempCondition.compareOp=lessThan;
			else if (strcmp(word.c_str(),"<=")==0) tempCondition.compareOp=lessEqual;
			else if (strcmp(word.c_str(),"=")==0) tempCondition.compareOp=equalTo;
			else if (strcmp(word.c_str(),">=")==0) tempCondition.compareOp=largerEqual;
			else if (strcmp(word.c_str(),">")==0) tempCondition.compareOp=largerThan;
			else if (strcmp(word.c_str(),"<>")==0) tempCondition.compareOp=notEqual;
			else return;
			flag=getToken(temp,word);
			if (!flag) return;
			if (strcmp(word.c_str(),"\'")==0)
			{
				flag=getString(temp,word);
				if(!flag)return;
				tempCondition.conditionValue=word;
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),"\'")!=0) return;
			}
			else tempCondition.conditionValue=word;
			oConditionList.push_back(tempCondition);
			flag=getToken(temp,word);
			if(!flag){operation=DELETE;return;}
			while (strcmp(word.c_str(),"and")==0)
			{
				flag=getToken(temp,word);
				if (!flag) return;
				if (catalogManager.getAttributeIndex(oTable,word)==-1)
				{
					operation=ATTRIBUTEERROR;
					return;
				}
				tempCondition.attributeIndex=catalogManager.getAttributeIndex(oTable,word);
				flag=getToken(temp,word);
				if(!flag) return;
				if (strcmp(word.c_str(),"<")==0) tempCondition.compareOp=lessThan;
				else if (strcmp(word.c_str(),"<=")==0) tempCondition.compareOp=lessEqual;
				else if (strcmp(word.c_str(),"=")==0) tempCondition.compareOp=equalTo;
				else if (strcmp(word.c_str(),">=")==0) tempCondition.compareOp=largerEqual;
				else if (strcmp(word.c_str(),">")==0) tempCondition.compareOp=largerThan;
				else if (strcmp(word.c_str(),"<>")==0) tempCondition.compareOp=notEqual;
				else return;
				flag=getToken(temp,word);
				if(!flag) return;
				if (strcmp(word.c_str(),"\'")==0)
				{
					flag=getString(temp,word);
					if(!flag)return;
					tempCondition.conditionValue=word;
					flag=getToken(temp,word);
					if (!flag) return;
					if (strcmp(word.c_str(),"\'")!=0) return;
				}
				else tempCondition.conditionValue=word;
				oConditionList.push_back(tempCondition);
				flag=getToken(temp,word);
				if(!flag){operation=DELETE;return;}
			}
		}
	}
	/*------------insert--------------*/
	if (strcmp(word.c_str(),"insert")==0)
	{
		flag=getToken(temp,word);
		if (!flag) return;
		//insert into
		if (strcmp(word.c_str(),"into")==0)
		{
			operation=INSERTERROR;
			flag=getToken(temp,word);
			if (!flag) return;
			oTableName=word;
			if (!catalogManager.tableCheck(word))
			{
				operation=TABLENOTEXIST;
				return;
			}
			oTable=catalogManager.getTable(oTableName);
			flag=getToken(temp,word);
			if(!flag)return;
			//insert into values
			if (strcmp(word.c_str(),"values")!=0) return;
			flag=getToken(temp,word);
			if(!flag)return;
			if (strcmp(word.c_str(),"(")!=0) return;
			flag=getToken(temp,word);
			if(!flag) return;
			if (strcmp(word.c_str(),"\'")==0)
			{
				flag=getString(temp,word);
				if(!flag) return;
				tempTuple=word;
				flag=getToken(temp,word);
				if (!flag)return;
				if (strcmp(word.c_str(),"\'")!=0) return;
			}
			else tempTuple=word;
			flag=getToken(temp,word);
			if (!flag) return;
			oTuple.attributeValueList.push_back(tempTuple);
			while (strcmp(word.c_str(),",")==0)
			{
				flag=getToken(temp,word);
				if (!flag) return;
				if (strcmp(word.c_str(),"\'")==0) 
				{
					flag=getString(temp,word);
					if(!flag) return;
					tempTuple=word;
					flag=getToken(temp,word);
					if (!flag) return;
					if (strcmp(word.c_str(),"\'")!=0) return;
				}
				else tempTuple=word;
				oTuple.attributeValueList.push_back(tempTuple);
				flag=getToken(temp,word);
				if(!flag) return;
			}
			if (word!=")") return;
			if (oTuple.attributeValueList.size()!=oTable.attributeNum)
			{
				operation=INSERTNUMBERERROR;
				return;
			}
			flag=getToken(temp,word);
			for (unsigned int i=0;i<oTable.attributeList.size();i++)
			{
				if (oTable.attributeList[i].isPrimaryKey)
				{
					primaryKeyIndex=i;
					tempCondition.attributeIndex=i;
					tempCondition.compareOp=equalTo;
					tempCondition.conditionValue=oTuple.attributeValueList[i];
					oConditionList.push_back(tempCondition);
				}
				if (oTable.attributeList[i].isPrimaryKey==false && oTable.attributeList[i].isUnique)
				{
					uniqueKeyIndex=i;
					tempCondition.attributeIndex=i;
					tempCondition.compareOp=equalTo;
					tempCondition.conditionValue=oTuple.attributeValueList[i];
					uniqueCondition.push_back(tempCondition);
				}
			}
			if (!flag) 
			{
				operation=INSERT;
				return;
			}
		}
	}
	/*------------drop--------------*/
	if (strcmp(word.c_str(),"drop")==0)
	{
		flag=getToken(temp,word);
		if(!flag) return;
		if (strcmp(word.c_str(),"table")==0)
		{
			operation=DROPTABLEERROR;
			flag=getToken(temp,word);
			if(!flag) return;
			oTableName=word;
			if (!catalogManager.tableCheck(word))
			{
				operation=TABLENOTEXIST;
				return;
			}
			oTable=catalogManager.getTable(oTableName);
			flag=getToken(temp,word);
			if(!flag) operation=DROPTABLE;
		}
		else if (strcmp(word.c_str(),"index")==0)
		{
			operation=DROPINDEXERROR;
			flag=getToken(temp,word);
			if (!flag) return;
			oIndexName=word;
			if (!catalogManager.indexCheck(oIndexName))
			{
				operation=INDEXNOTEXIST;
				return;
			}
			flag=getToken(temp,word);
			if (!flag) operation=DROPINDEX;
		} 
	}
	return;
}

#endif
