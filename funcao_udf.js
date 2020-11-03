function transform(line) {
var values = line.split(',');

var obj = new Object();
obj.ID_MARCA = values[0];
obj.MARCA = values[1];
obj.ID_LINHA = values[2];
obj.LINHA = values[3];
obj.DATA_VENDA = values[4].substring(6,10)+'-'+values[4].substring(3,5)+'-'+values[4].substring(0,2);
obj.QTD_VENDA = values[5];
var jsonString = JSON.stringify(obj);

return jsonString;
}