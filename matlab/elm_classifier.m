classdef elm_classifier
    methods(Static)
        function [error,data,msg]=elm(X,Y,K,niterations,verbose)
                if isempty(K)
                    K=[10]; % Neurones capa intermitja
                end
                if isempty(niterations)
                    niterations=10; % Iteracions
                end
                
                if isa(X,'table')
                    X=double(table2array(X));
                end
                
                if ~isa(Y,'double')
                    Y=double(Y);
                end
                
                %Remove NaN
                okrows= ~any(isnan(X),2) & ~any(isnan(Y),2);
                X=X(okrows,:);

                %Normalize Z score (X-mean(x)/std(x))
                [X,X_mu,X_sigma]=zscore(X);
                [N, d]=size(X);            % d: dimensi� features     N: observacions

                y=Y(okrows); %continuos.
                % compute min max of response.
                min_y=min(y);
                max_y=max(y);
                %Normalize Z score (y-mean(y)/std(y))
                [y_norm,y_mu,y_sigma]=zscore(y);
                T=y_norm;

                %
                % %Preparacion parallel 
                % p = gcp;
                % delete(p) %apagar cualquier pool viejo activo.
                % numLogicalCpus=eval('java.lang.Runtime.getRuntime().availableProcessors');
                % LASTN = maxNumCompThreads(numLogicalCpus);%si tiene hyperthreating forzamos que matlab lo use.
                % c = parcluster('local');
                % c.NumWorkers = numLogicalCpus; 
                % saveProfile(c);
                % parpool(c,numLogicalCpus);%Creamos un parallel pool de tantos cores tengamos (incluido logicos)

                %Fix seed
                rng(1)

                %  ________________________________________
                
                table_results=cell2table(cell(niterations,7));
                table_results.Properties.VariableNames={'yhat','rmse','norm_rmse','b','W','B','K'};
                %Clear
                clear rmse b W u H pI B A res;
                %parfor i=1:niterations %parallel
                for k=1:size(K,2) %K
                   for iter=1:niterations %iter
                        b=1*randn(K(k),1);  % Bias, escollit aleatoriament

                        W=randn(d,K(k)); % Pesos entrada, escollits aleatoriament
                        u=ones(N,1);
                        H=X*W+kron(u,b'); % Matriu de pesos * entrades + bias
                        % _________________________________________
                        %  Calcul dels pesos  B de sortida___________
                        % (B normalment es una matriu: multiclassificacio

                        pI=pinv(H'*H)*H'; %inv no funciona bien en todos los casos
                        B=pI*T;  
                        % Calcul dels resultats:  A es H!!!
                        %A=X*W+kron(u,b'); 
                        %yhat=A*B;
                        yhat=H*B;

                        % ______
                        %  RMSE
                        I = ~isnan(y) & ~isnan(yhat);
                        yok = y(I); yhat = yhat(I);
                        %De-normalize
                        yhat=(yhat*y_sigma)+y_mu;
                        %Calculate rmse
                        rmse=sqrt(sum((yok(:)-yhat(:)).^2)/numel(yok));
                        %Calculate normalized rmse
                        norm_rmse=rmse/(max_y-min_y);
                        %Store results.
                        table_results(iter,:).yhat={yhat};
                        table_results(iter,:).rmse={rmse};
                        table_results(iter,:).norm_rmse={norm_rmse};
                        table_results(iter,:).b={b};
                        table_results(iter,:).W={W};
                        table_results(iter,:).B={B};
                        table_results(iter,:).K={K(k)};
                    end
                end

                %Get lowest rmse
                rmse_array=cell2mat(table_results{:,'rmse'});
                posmin=find(rmse_array==min(rmse_array));
                if(length(posmin)>1)
                  posmin=posmin(1);
                end
                table_results(posmin,:)
                if verbose
                    disp(['Minimal RMSE(',num2str(table_results{posmin,'rmse'}{:}),') for K:',num2str(table_results{posmin,'K'}{:}),' iteration:',num2str(posmin)])
                end
                
                data.train_yhat={table_results(posmin,yhat)};
                data.train_rmse={table_results(posmin,rmse)};
                data.train_norm_rmse={table_results(posmin,norm_rmse)};
                data.model.b={table_results(posmin,b)};
                data.model.B={table_results(posmin,B)};
                data.model.W={table_results(posmin,W)};
                data.model.K={table_results(posmin,K(k))};
                data.model.y_mu={y_mu};
                data.model.y_sigma={y_sigma};
                data.model.X_mu={X_mu};
                data.model.X_sigma={X_sigma};
                msg="ok";
                error=false;
        end
        function [error,data,msg]=elm_predict(X,Y,model,verbose)
                if isa(X,'table')
                    X=double(table2array(X));
                end
                
                if ~isa(Y,'double')
                    Y=double(Y);
                end
                
                %Remove NaN
                okrows= ~any(isnan(X),2) & ~any(isnan(Response),2);
                X=X(okrows,:);

                %Normalize Z score (X-mean(x)/std(x))
                X=(X-model.X_mu)/model.X_sigma;
                [N, d]=size(X);            % d: dimensi� features     N: observacions
                
                y=Y(okrows); %continuos.
                % compute min max of response.
                min_y=min(y);
                max_y=max(y);
                %Normalize Z score (y-mean(y)/std(y))
                y_norm=(y-model.y_mu)/model.y_sigma;
                
                % Recuperar datos del modelo
                b=model.b{1};
                W=model.W{1};
                B=model.B{1};

                u=ones(N,1);
                A=X*W+kron(u,b');
                yhat_norm=A*B;
                %RMSE
                I = ~isnan(y_norm) & ~isnan(yhat_norm);
                yok = y_norm(I); yhat_norm = yhat_norm(I);
                %De-normalize
                yhat=(yhat_norm*model.y_sigma)+model.y_mu;
                %Calculate rmse
                norm_rmse=sqrt(sum((yok(:)-yhat_norm(:)).^2)/numel(yok));
                %Calculate normalized rmse
                rmse=norm_rmse/(max_y-min_y);
                data.rmse=rmse;
                data.norm_rmse=norm_rmse;
                data.yhat=yhat;
                data.yhat_norm=yhat_norm;
                msg="ok";
                error=false;
        end
    end
end