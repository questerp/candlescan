// Copyright (c) 2021, ovresko and contributors
// For license information, please see license.txt

frappe.ui.form.on('Candlescan Settings', {
	 refresh: function(frm) {
		frm.add_custom_button("Reload scanners",
							() => {
								frappe.call({
									method: 'candlescan.candlescan.doctype.candlescan_settings.candlescan_settings.start_scanners',
									args: { },
									callback(r) {
										alert("Scanners running");
										if (!r.exc) {
												frm.reload_doc();
											}
									}
								});
							});
	 }
});
