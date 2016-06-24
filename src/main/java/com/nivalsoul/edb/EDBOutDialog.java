package com.nivalsoul.edb;


import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Text;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

public class EDBOutDialog extends BaseStepDialog
        implements StepDialogInterface {
    private static Class<?> PKG = EDBOutMeta.class;
    private EDBOutMeta input;

    private Label solrUrl;
    private Label solrCollection;

    private Text solrUrlText;
    private Text solrCollectionText;

    private FormData solrUrlFormData;
    private FormData solrUrlTxtFormData;
    private FormData solrCollectionFormData;
    private FormData solrCollectionTxtFormData;

    public EDBOutDialog(Shell parent, Object in, TransMeta transMeta, String sname) {
        super(parent, (BaseStepMeta) in, transMeta, sname);
        this.input = ((EDBOutMeta) in);
    }

    public String open() {
        Shell parent = getParent();
        Display display = parent.getDisplay();

        this.shell = new Shell(parent, 3312);
        this.props.setLook(this.shell);
        setShellImage(this.shell, this.input);

        ModifyListener lsMod = new ModifyListener() {
            public void modifyText(ModifyEvent e) {
                EDBOutDialog.this.input.setChanged();
            }
        };
        this.changed = this.input.hasChanged();
        FormLayout formLayout = new FormLayout();
        formLayout.marginWidth = 5;
        formLayout.marginHeight = 5;
        
        this.shell.setLayout(formLayout);

        this.shell.setText("ES Config");

        int middle = this.props.getMiddlePct();
        int margin = 4;

        this.wlStepname = new Label(this.shell, 131072);
        this.wlStepname.setText(Messages.getString("System.Label.StepName"));
        this.props.setLook(this.wlStepname);
        this.fdlStepname = new FormData();
        this.fdlStepname.left = new FormAttachment(0, 0);
        this.fdlStepname.right = new FormAttachment(middle, -margin);
        this.fdlStepname.top = new FormAttachment(0, margin);
        this.wlStepname.setLayoutData(this.fdlStepname);

        this.wStepname = new Text(this.shell, 18436);
        this.wStepname.setText(this.stepname);
        this.props.setLook(this.wStepname);
        this.wStepname.addModifyListener(lsMod);
        this.fdStepname = new FormData();
        this.fdStepname.left = new FormAttachment(middle, 0);
        this.fdStepname.top = new FormAttachment(0, margin);
        this.fdStepname.right = new FormAttachment(100, 0);
        this.wStepname.setLayoutData(this.fdStepname);

        //For Solr Url
        this.solrUrl = new Label(this.shell, 131072);
        this.solrUrl.setText("ES Address");
        this.props.setLook(this.solrUrl);
        this.solrUrlFormData = new FormData();
        this.solrUrlFormData.left = new FormAttachment(0, 0);
        this.solrUrlFormData.right = new FormAttachment(middle, -margin);
        this.solrUrlFormData.top = new FormAttachment(this.wStepname, margin);
        this.solrUrl.setLayoutData(this.solrUrlFormData);

        this.solrUrlText = new Text(this.shell, 18436);
        this.props.setLook(this.solrUrlText);
        this.solrUrlText.addModifyListener(lsMod);
        this.solrUrlTxtFormData = new FormData();
        this.solrUrlTxtFormData.left = new FormAttachment(middle, 0);
        this.solrUrlTxtFormData.right = new FormAttachment(100, 0);
        this.solrUrlTxtFormData.top = new FormAttachment(this.wStepname, margin);
        this.solrUrlText.setLayoutData(this.solrUrlTxtFormData);
        
        //For SolrCollection
        this.solrCollection = new Label(this.shell, 131072);
        this.solrCollection.setText("IndexInfo");
        this.props.setLook(this.solrCollection);
        this.solrCollectionFormData = new FormData();
        this.solrCollectionFormData.left = new FormAttachment(0, 0);
        this.solrCollectionFormData.right = new FormAttachment(middle, -margin);
        this.solrCollectionFormData.top = new FormAttachment(this.solrUrlText, margin);
        this.solrCollection.setLayoutData(this.solrCollectionFormData);
        this.solrCollectionText = new Text(this.shell, 18436);
        this.props.setLook(this.solrCollectionText);
        this.solrCollectionText.addModifyListener(lsMod);
        this.solrCollectionTxtFormData = new FormData();
        this.solrCollectionTxtFormData.left = new FormAttachment(middle, 0);
        this.solrCollectionTxtFormData.right = new FormAttachment(100, 0);
        this.solrCollectionTxtFormData.top = new FormAttachment(this.solrUrlText, margin);
        this.solrCollectionText.setLayoutData(this.solrCollectionTxtFormData);

        // For Button
        this.wOK = new Button(this.shell, 8);
        this.wOK.setText(BaseMessages.getString(PKG, "System.Button.OK", new String[0]));

        this.wCancel = new Button(this.shell, 8);
        this.wCancel.setText(BaseMessages.getString(PKG, "System.Button.Cancel", new String[0]));

        BaseStepDialog.positionBottomButtons(this.shell, new Button[]{this.wOK, this.wCancel}, margin, this.solrCollectionText);

        this.lsCancel = new Listener() {
            public void handleEvent(Event e) {
                EDBOutDialog.this.cancel();
            }
        };
        this.lsOK = new Listener() {
            public void handleEvent(Event e) {
                EDBOutDialog.this.ok();
            }
        };

        this.wCancel.addListener(13, this.lsCancel);
        this.wOK.addListener(13, this.lsOK);

        this.lsDef = new SelectionAdapter() {
            public void widgetDefaultSelected(SelectionEvent e) {
                EDBOutDialog.this.shell.dispose();
            }
        };
        this.wStepname.addSelectionListener(this.lsDef);

        this.shell.addShellListener(new ShellAdapter() {
            public void shellClosed(ShellEvent e) {
                EDBOutDialog.this.shell.dispose();
            }
        });
        setSize();

        getData();
        this.input.setChanged(this.changed);

        this.shell.open();
        while (!this.shell.isDisposed()) {
            if (!display.readAndDispatch())
                display.sleep();
        }
        return this.stepname;
    }

    public void getData() {
        this.wStepname.selectAll();
        if (this.input.getUrl() != null & this.input.getCollection() != null) {
            this.solrUrlText.setText(this.input.getUrl());
            this.solrCollectionText.setText(this.input.getCollection());
        }
    }

    private void cancel() {
        this.stepname = null;
        this.input.setChanged(this.changed);
        dispose();
    }

    private void ok() {
        this.stepname = this.wStepname.getText();
        this.input.setUrl(this.solrUrlText.getText());
        this.input.setCollection(this.solrCollectionText.getText());

        dispose();
    }

}