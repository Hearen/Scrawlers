var page = require('webpage').create(),
system = require('system'),
url;

if(system.args.length === 1)
{
	console.log("parameters not enough");
	phantom.exit(1);
}
else
{
	url = system.args[1];
	page.open(url, function (status) 
	{
		if(status === 'success')
		{
			var content = page.evaluate(function () 
			{
				return document.body.innerHTML;
			});
			console.log('Page content is ' + content);
			phantom.exit();
		}
		console.log("load page failed!");
		phantom.exit(1);
	});
}
/*
//console.log(system.args.length);
if(system.args.length === 1)
{
	phntom.exit(1);
}
else
{
	//address = system.args[1];
	address = 'https://www.baidu.com/';
	console.log("trying to open " + address);
	page.open(address, function(status)
	{
				var sc = page.evaluate(function()
				{
					console.log(document.body.innerHTML);
					return document.body.innerHTML;
				});
		if(status !== 'success')
		{
			phantom.exit();
		}
		else
		{
			
			/*window.setTimeout(function()
				{
					console.log(sc);
					phantom.exit();
				}, 1000);
				
		}
	}
}
*/