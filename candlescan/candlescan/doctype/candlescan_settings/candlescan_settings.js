// Copyright (c) 2021, ovresko and contributors
// For license information, please see license.txt

frappe.ui.form.on('Candlescan Settings', {
	 refresh: function(frm) {
		frm.add_custom_button("Reload scanners",
			() => {
				frappe.call({
					method: 'candlescan.candlescan_service.start_scanners',
					args: { },
					callback(r) {
						console.log(r);
						alert("Scanners running",r.message);
						if (!r.exc) {
								frm.reload_doc();
							}
					}
				});
			});
		 frm.add_custom_button("Start services",
			() => {
				frappe.call({
					method: 'candlescan.candlescan_service.start_services',
					args: { },
					callback(r) {
						console.log(r);
						alert("services running",r.message);
						if (!r.exc) {
								frm.reload_doc();
							}
					}
				});
			});
		 
	 }
});
