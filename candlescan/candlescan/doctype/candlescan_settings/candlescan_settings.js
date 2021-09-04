// Copyright (c) 2021, ovresko and contributors
// For license information, please see license.txt

frappe.ui.form.on('Candlescan Settings', {
	 refresh: function(frm) {
		frm.add_custom_button("Commit",
			() => {
				frappe.call({
					method: 'candlescan.candlescan.doctype.candlescan_settings.candlescan_settings.commit',
					args: { },
					callback(r) {
						alert("commit");
					}
				});
			});
	 }
});
