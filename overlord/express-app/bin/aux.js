function getOrderedAccept(req){
	var list = {};
	

	list['html'] = req.indexOf('html') > -1 ? req.indexOf('html') : null;
	list['json'] = req.indexOf('json') > -1 ? req.indexOf('json') : null;
	list['xml'] = req.indexOf('xml') > -1 ? req.indexOf('xml') : null;

}

getOrderedAccept("ñlaskjfñlasdkjhtml");