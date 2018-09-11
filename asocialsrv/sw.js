

/*******************************************************
const shortcuts = {"?": "prod-index", "index2": "prod-index2", "index": "prod-index", "d": "demo-index", "admin": "prod-index2"};
const inb = 4; 
const uib = [11];
const cp = "tsw"; 
const dyn_appstore = "https://127.0.0.1/cp/"
const static_appstore = "https://127.0.0.1/cp/$ui/"

//const CP = cp ? '/' + cp + '/' : '/'; 
//const CPOP = CP + '$op/'; 
//const CPUI = CP + '$ui/'; 
//const BC = inb + '.' + uib[0]; 
//const x = CPUI + BC +'/';
const lres = [
	x + "helper.js",
	x + "home.html",
	x + "favicon.ico"
];
*/

const CP = cp ? '/' + cp + '/' : '/'; 
const CPOP = CP + '$op/'; 
const CPUI = CP + '$ui/'; 
const BC = inb + '.' + uib[0]; 

const CACHENAME =  (cp ? cp : "root") + "_" + BC;	
const TRACEON = true;	// trace sur erreurs
const TRACEON2 = false; // trace sur actions normales
const VCACHES = { }
const BUILDS = { }
const TIME_OUT_MS = 30000;
const encoder = new TextEncoder("utf-8");

let installReport = "?";
let mycaches = []

for(let i = 0, b = 0; b = uib[i]; i++) {
	VCACHES[(cp ? cp : "root") + "_" + inb + "." + b] = true;
	BUILDS[inb + "." + b] = true;
}

// installation des ressources requises pour la build BC
this.addEventListener('install', function(event) {
	if (TRACEON) console.log("Install de " + BC);
	event.waitUntil(
		caches.open(CACHENAME)
		.then(cache => {
			if (TRACEON2) console.log("Install addAll demandé ... " + BC + " - " + lres.length + " resources");
			const res = cache.addAll(lres);
			installReport = "Install addAll OK " + BC + " - " + lres.length + " resources";
			if (TRACEON2) console.log(installReport);
			return res;
		}).catch(error => {
			// Une des URLs citées est NOT FOUND, on ne sait pas laquelle
			installreport = "Install addAll KO " + BC + " - " + error.message;
			if (TRACEON) console.log(installReport);
		})
	);
});

// Suppression des caches obsolètes lors d'une activation
this.addEventListener('activate', function(event) {
	event.waitUntil(
		caches.keys()
		.then(cacheNames => {
				if (TRACEON) console.log("Suppression des caches obsolètes sur activation de " + BC);
				return Promise.all(
					cacheNames.map(cacheName => {
						if (!VCACHES[cacheName]) {
							if (TRACEON) console.log("Suppression de " + cacheName);
							return caches.delete(cacheName);
						} else
							mycaches.push(cacheName);			
					})
				);
		}).catch(error => {
			if (TRACEON) console.log("ERREUR sur suppression des caches obsolètes sur activation de " + BC + " - " + error.message);
		})
	);
});

const nf404 = function(m, u) {
	let txt = encoder.encode(m + " : " + u);
	const headers = new Headers();
	headers.append("Content-Type", "text/plain; charset=utf-8");
	headers.append("Cache-control", "no-store");
	headers.append("Content-Length", "" + txt.length);
	return new Response(txt, {status:404, statusText:"Not Found", headers:headers});
}

const infoSW = function() {
	let txt = encoder.encode(JSON.stringify({inb:inb, uib:uib}));
	const headers = new Headers();
	headers.append("Content-Type", "text/plain");
	headers.append("Cache-control", "no-store");
	headers.append("Content-Length", "" + txt.length);
	return new Response(txt, {status:200, statusText:"OK", headers:headers});
}

const myCaches = async function() {
	let cache = await caches.open(CACHENAME);
	let keys = await cache.keys();
	let lst = [];
	lst.push("mycaches: " + mycaches.join("  "));
	lst.push("installReport: " + installReport);
	lst.push("keys: " + keys.length);
	for(let i = 0; i < keys.length; i++) {
		let req= keys[i]
		lst.push(req.url);
	}
	let txt = encoder.encode(lst.join("\n"));
	const headers = new Headers();
	headers.append("Content-Type", "text/plain");
	headers.append("Cache-control", "no-store");
	headers.append("Content-Length", "" + txt.length);
	return new Response(txt, {status:200, statusText:"OK", headers:headers});
}

const fetchTO = async function(req, timeout) {
	let resp;
	try {
		let controller, signal, tim;
		if (timeout) {
			controller = new AbortController();
			signal = controller.signal;
			tim = setTimeout(() => { controller.abort(); }, timeout);
		}
		/*
		 * exception : Cannot construct a Request with a Request whose mode is 'navigate' and a non-empty RequestInit
		 * quand on fetch une page. 
		 * D'ou ne passer que l'url quand on fetch une ressouce (timeout != 0)
		 */
		resp = await fetch(timeout ? req.url : req, {signal});
		if (timeout && tim) 
			clearTimeout(tim);
		if (resp && resp.ok) {
			if (TRACEON2) console.log("fetch OK du serveur : " + req.url);
		} else {
			if (TRACEON) console.log("fetch KO du serveur : " + req.url);
		}
		return resp;
	} catch (e) {
		if (timeout)
			return nf404("Exception : " + e.message + " sur fetch ressource", req.url);
		else
			return resp;
	}
}

// recherche dans les caches : si build recherche au serveur et garde la réponse en cache de la build citée
const fetchFromCaches = async function(req, build) {
	let u = req.url;
	let search = "";
	let i = req.url.lastIndexOf("?")
	if (i != -1) {
		u = req.url.substring(0,i);
		search = req.url.substring(i);
	}
	let html = u.endsWith(".html");
	let resp = await caches.match(u);
	if (resp && resp.ok) {
		if (TRACEON2) console.log("fetch OK du CACHE : " + req.url);
		return resp;
	}
	resp = await fetchTO(req.clone(), TIME_OUT_MS);
	if (!resp || !resp.ok || !build)
		return resp;
	let cachename = (cp ? cp : "root") + "_" + build;
	let cache = await caches.open(cachename);
	await cache.put(req.clone, resp.clone());
	if (TRACEON2) console.log("PUT dans CACHE : " + req.url);
	return resp;
}

//recherche dans la cache de build : si toCache recherche au serveur et garde la réponse en cache de la build citée
const fetchFromCache = async function(req, build, toCache) {
	let cachename = (cp ? cp : "root") + "_" + build;
	let cache = await caches.open(cachename);
	let resp = await cache.match(req.url, {ignoreSearch:true, ignoreMethod:true, ignoreVary:true});
	if ((resp && resp.ok) || !toCache) 
		return resp;		
	resp = await fetchTO(req.clone(), TIME_OUT_MS);
	if (!resp || !resp.ok)
		return resp;
	await cache.put(req.clone, resp.clone());
	return resp;
}

const fetchHome = async function(org, home, build, mode, qs){
    let x = "$build=" + BC + "&$org=" + org + "&$home=" + home + "&$mode=" + mode + "&$cp=" + cp + "&$appstore=" + dyn_appstore + "&$maker=Service-Worker"  
    let redir = static_appstore + build + "/" + home + ".html" + (qs ? qs + "&" : "?") + x
	let txt = encoder.encode("<html><head><meta http-equiv='refresh' content='0;URL=" + redir + "'></head><body></body></html>");
	const headers = new Headers();
	headers.append("Content-Type", "text/html");
	headers.append("Cache-control", "no-cache, no-store, must-revalidate");
	headers.append("Content-Length", "" + txt.length);
	return new Response(txt, {status:200, statusText:"OK", headers:headers});
}

this.addEventListener('fetch', event => {
	let now = new Date().toISOString();
	let url = event.request.url;
	if (TRACEON2) console.log("fetch event " + now + " sur: " + url);
	let i = url.indexOf("//");
	let j = url.indexOf("/", i + 2);
	let site;		// https://localhost:443     jusqu'au / SANS /
	let path;		// ce qui suit le site AVEC /
	if (j != -1) {
		site = url.substring(0, j);
		path = j == url.length - 1 ? "/" : url.substring(j);
	} else {
		path = "/";
		site = url;
	}
	
	if (path == CP + "$infoSW") {
		event.respondWith(infoSW());
		return;
	}

	if (path == CP + "$infoCACHES") {
		event.respondWith(myCaches());
		return;
	}

	if (path.startsWith(CP + "$info/") || path == CP + "$swjs" || path.startsWith(CP + "$sw.html")) {
		event.respondWith(fetchTO(event.request, TIME_OUT_MS));
		return;
	}

	if (path.startsWith(CPOP)) {
		event.respondWith(fetchTO(event.request, 0));
		return;
	}

	if (path.startsWith(CPUI)) {
		let p = path.substring(CPUI.length);
		let i = p.indexOf("/");
		let b = i != -1 ? p.substring(0, i) : "";
		event.respondWith(fetchFromCaches(event.request, b != BC ? b : null));
		return;
	}

	if (path.startsWith(CP)) {
		let p = path.substring(CP.length);
		let j = p.lastIndexOf("?");
		let home1 = p;
		let qs = ""
		if (j != -1) {
			home1 =  p.substring(0, j);
			qs = p.substring(j);
		}

		let h = analyseHome(home1, qs, shortcuts);
		// {home:home, org:org, build:build, mode:mode}
		
		if (h.mode == 2) // redirect 
			event.respondWith(fetchHome(h.org, h.home, h.build, h.mode, qs));
		else // recharge depuis le magasin d'application
			event.respondWith(fetchTO(event.request, TIME_OUT_MS));
		return;
	}
	
	event.respondWith(nf404("Syntaxe URL non reconnue", url));
});

const analyseHome = function(home1, qs, shortcuts) {
	let i, home, org, mode, build;
	
	let home2;
	i = home1.lastIndexOf(".");
	if (i == -1) {
		mode = 1;
		home2 = home1;
	} else {
		let ext = home1.substring(i + 1);
		home2 = home1.substring(0, i);
		if (ext.startsWith("a")) 
			mode = 2;
		else if (ext.startsWith("i") || ext == "html")
			mode = 0;
		else 
			mode = 1;				
	}

	let orgHome = home2;
	if (!home2)
		orgHome = shortcuts["?"];
	else {
		i = home2.indexOf("-");
		if (i == -1) {
			let x = shortcuts[home2];
			orgHome = x ? x : home2 + "-index";
		}
	}
	i = orgHome.indexOf("-");
	org = orgHome.substring(0, i);
	home = orgHome.substring(i + 1);
	
	let breq = null;
	if (qs) {
	    i = qs.indexOf("build=");
	    if (i != -1) {
	        let j = qs.indexOf("&", i + 6);
	        x = j == -1 ? qs.substring(i + 6, j) : qs.substring(i + 6);
	        if (x) {
	            let y = x.split(".");
	            try {
	            	breq = [0, 0, 0];
	                breq[0] = y.length > 1 ? y[0] : 0;
	    	        breq[1] = y.length >= 2 ? y[1] : 0;
	    	        breq[0] = y.length >= 3 ? y[2] : 0;
	                if (breq[0] < 1 || breq[1] < 0 || breq[2] < 0)
	                    breq = null
	            } catch (e) {
	            	breq = null;
	            }
	        }
	    }
	}
	build = "" + inb + "." + (breq ? breq[1] : uib[0])
	return {home:home, org:org, mode:mode, build:build}
}


