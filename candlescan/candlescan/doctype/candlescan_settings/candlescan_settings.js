// Copyright (c) 2021, ovresko and contributors
// For license information, please see license.txt

frappe.ui.form.on('Candlescan Settings', {
	 refresh: function(frm) {
		frm.add_custom_button("Commit",
			() => {
				frappe.call({
					method: 'candlescan.candlescan.doctype.candlescan_settings.commit',
					args: { },
					callback(r) {
						alert("commit");
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
